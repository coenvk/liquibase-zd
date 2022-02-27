package liquibase.ext.change.update

import liquibase.Scope
import liquibase.change.DatabaseChangeProperty
import liquibase.change.custom.CustomTaskChange
import liquibase.change.custom.CustomTaskRollback
import liquibase.database.Database
import liquibase.database.core.OracleDatabase
import liquibase.database.core.PostgresDatabase
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.CustomChangeException
import liquibase.exception.ValidationErrors
import liquibase.ext.util.KotlinExtensions.getAll
import liquibase.logging.Logger
import liquibase.resource.ResourceAccessor
import liquibase.structure.core.Column
import java.sql.*

class BulkColumnCopyChange : CustomTaskChange, CustomTaskRollback {
    var catalogName: String? = null
    var schemaName: String? = null
    var tableName: String? = null
    var fromColumns: String? = null
    var toColumns: String? = null
    var chunkSize: Long? = DEFAULT_CHUNK_SIZE

    var sleepTime: Long? = DEFAULT_SLEEP_TIME
        @DatabaseChangeProperty(requiredForDatabase = [])
        get() {
            if (field == null) return DEFAULT_SLEEP_TIME
            return field
        }

    private var resourceAccessor: ResourceAccessor? = null

    private val fromArray: Array<Column> by lazy {
        Column.arrayFromNames(fromColumns)
    }

    private val toArray: Array<Column> by lazy {
        Column.arrayFromNames(toColumns)
    }

    private val updateColumnsString: String by lazy {
        toArray.zip(fromArray).joinToString(separator = ", ") { (f, t) -> "${f.name} = ${t.name}" }
    }

    private val fullName: String by lazy {
        PostgresDatabase().escapeTableName(catalogName, schemaName, tableName)
    }

    private val whereClauseString: String by lazy {
        val t = toArray.first()
        val f = fromArray.first()

        "(${t.name} IS NULL and ${f.name} IS NOT NULL)"
    }

    override fun setFileOpener(resourceAccessor: ResourceAccessor?) {
        this.resourceAccessor = resourceAccessor
    }

    override fun getConfirmationMessage(): String =
        "Successfully migrated $fromColumns to $toColumns in $tableName"

    override fun setUp() = Unit

    override fun validate(db: Database): ValidationErrors {
        val errors = ValidationErrors()
        // make use of short-circuit
        if (db !is PostgresDatabase ||
            errors.checkAndAddSimpleErrors() ||
            errors.checkAndAddDuplicateErrors(fromArray, "fromColumns") ||
            errors.checkAndAddDuplicateErrors(toArray, "toColumns") ||
            errors.checkAndAddNEqFromAndTo() ||
            errors.checkAndAddCrossingColumns()
        ) {
            return errors
        }

        return errors
    }

    private fun ValidationErrors.checkAndAddCrossingColumns(): Boolean = run {
        val toSet = toArray.toSet()
        fromArray.forEachIndexed { i: Int, col: Column ->
            if (col == toArray[i]) {
                addError("Column $col should not be migrated to itself")
            } else if (col in toSet) {
                addError("Migration of $col crosses, which is not possible")
            }
        }
        hasErrors()
    }

    private fun ValidationErrors.checkAndAddNEqFromAndTo(): Boolean = run {
        val unequal = fromArray.size != toArray.size
        if (unequal) {
            addError("Both in and output columns require a 1:1 relationship")
        }
        unequal
    }

    private fun ValidationErrors.checkAndAddSimpleErrors(): Boolean = run {
        if (tableName.isNullOrEmpty()) {
            addError("Table is not provided")
        }

        val nullCount = setOf(fromColumns, toColumns).filter { it.isNullOrEmpty() }.count()
        if (nullCount >= 1) {
            addError("Both fromColumns and toColumns need to be provided")
        }

        if (chunkSize == null || chunkSize!! <= 0L) {
            addError("chunkSize should be provided as a positive long")
        }

        if (sleepTime != null && sleepTime!! < 0L) {
            addError("sleepTime cannot be negative")
        }

        hasErrors()
    }

    private fun ValidationErrors.checkAndAddDuplicateErrors(arr: Array<Column>, name: String): Boolean = run {
        val unequal = arr.toSet().size != arr.size
        if (unequal) {
            addError("Duplicate elements in $name")
        }
        unequal
    }

    override fun execute(db: Database) {
        try {
            when (db) {
                is PostgresDatabase -> {
                    LOG.info("Executing BatchMigrationChange on supported Database")
                    startMigration(db)
                }
                else -> {
                    LOG.info("Skipping BatchMigrationChange due to non-supported Database")
                }
            }
        } catch (e: CustomChangeException) {
            throw e
        }
    }

    @Suppress("ThrowsCount", "NestedBlockDepth")
    private fun startMigration(db: Database) {
        if (db.connection == null || db.connection.isClosed) {
            LOG.severe("Not connected to database")
            throw CustomChangeException("Not connected to database")
        }

        val conn = db.connection as JdbcConnection
        // Fetch only the rows where not all values are synced yet
        var query =
            "UPDATE $fullName SET $updateColumnsString WHERE %1\$s IN (SELECT %1\$s FROM $fullName WHERE $whereClauseString %2\$s);"
        query = when (db) {
            is OracleDatabase -> query.format("rowId", "FETCH FIRST $chunkSize ROWS ONLY")
            is PostgresDatabase -> {
                val rs = conn.metaData.getBestRowIdentifier(
                    catalogName,
                    schemaName,
                    tableName,
                    DatabaseMetaData.bestRowSession,
                    false
                )
                val columnNames = rs.getAll<String>(2).distinct().joinToString()
                query.format(columnNames, "LIMIT $chunkSize")
            }
            else -> throw UnsupportedOperationException()
        }

        try {
            if (db is OracleDatabase && !hasImmutableRowIds(conn)) {
                LOG.severe("rowIds are not immutable, migration strategy can not be applied")
                throw CustomChangeException("Database has no immutable rowIds")
            }

            var running = true
            while (running) {
                val n = executeMigrationChunk(query, conn)
                if (n == 0L) {
                    running = false
                } else {
                    if (sleepTime!! > 0L) {
                        Thread.sleep(sleepTime!!)
                    }
                }
            }
        } catch (e: CustomChangeException) {
            throw e
        }
    }

    private fun executeMigrationChunk(query: String, conn: JdbcConnection): Long {
        var stmt: PreparedStatement? = null
        try {
            stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
            LOG.info("Executing $stmt with query $query")
            val affectedRows = stmt.executeLargeUpdate()
            // serves as no-op when auto-commit = true
            conn.commit()
            LOG.info("Committed")
            return affectedRows
        } catch (e: SQLException) {
            throw CustomChangeException("Could not update $tableName in batch", e)
        } finally {
            stmt?.close()
        }
    }

    private fun hasImmutableRowIds(conn: JdbcConnection): Boolean {
        try {
            val dbMetaData: DatabaseMetaData = conn.metaData
            val lifetime = dbMetaData.rowIdLifetime
            return (lifetime == RowIdLifetime.ROWID_VALID_FOREVER)
        } catch (e: SQLException) {
            throw CustomChangeException("Failed to query for rowId lifetime", e)
        }
    }

    // We do not need rollbacks for this as it is not semantics, even though we 'could' by an inverse operation
    override fun rollback(db: Database) {
        LOG.info("Rollback requested: no-op result")
    }

    companion object {
        const val DEFAULT_CHUNK_SIZE = 1000L
        const val DEFAULT_SLEEP_TIME = 0L
        val LOG: Logger = Scope.getCurrentScope().getLog(BulkColumnCopyChange::class.java)
    }
}