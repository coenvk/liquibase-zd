package liquibase.ext.change.update

import liquibase.Scope
import liquibase.change.DatabaseChangeProperty
import liquibase.change.custom.CustomTaskChange
import liquibase.change.custom.CustomTaskRollback
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.CustomChangeException
import liquibase.exception.ValidationErrors
import liquibase.ext.util.KotlinExtensions.getAll
import liquibase.logging.Logger
import liquibase.resource.ResourceAccessor
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Statement

class BulkTableCopyChange : CustomTaskChange, CustomTaskRollback {
    var fromCatalogName: String? = null
    var fromSchemaName: String? = null
    var fromTableName: String? = null

    var toCatalogName: String? = null
    var toSchemaName: String? = null
    var toTableName: String? = null

    var chunkSize: Long? = DEFAULT_CHUNK_SIZE

    var sleepTime: Long? = DEFAULT_SLEEP_TIME
        @DatabaseChangeProperty(requiredForDatabase = [])
        get() {
            if (field == null) return DEFAULT_SLEEP_TIME
            return field
        }

    private var resourceAccessor: ResourceAccessor? = null

    private val fromFullName: String by lazy {
        PostgresDatabase().escapeTableName(fromCatalogName, fromSchemaName, fromTableName)
    }

    private val toFullName: String by lazy {
        PostgresDatabase().escapeTableName(toCatalogName, toSchemaName, toTableName)
    }

    override fun getConfirmationMessage(): String =
        "Successfully inserted records from $fromTableName into $toTableName"

    override fun setUp() = Unit

    override fun setFileOpener(resourceAccessor: ResourceAccessor?) {
        this.resourceAccessor = resourceAccessor
    }

    override fun validate(database: Database?): ValidationErrors {
        val errors = ValidationErrors()
        // make use of short-circuit
        if (database !is PostgresDatabase ||
            errors.checkAndAddSimpleErrors()
        ) {
            return errors
        }

        return errors
    }

    private fun ValidationErrors.checkAndAddSimpleErrors(): Boolean = run {
        checkRequiredField("fromTableName", fromTableName)
        checkRequiredField("toTableName", toTableName)

        if (chunkSize == null || chunkSize!! <= 0L) {
            addError("chunkSize should be provided as a positive long")
        }

        if (sleepTime != null && sleepTime!! < 0L) {
            addError("sleepTime cannot be negative")
        }

        hasErrors()
    }

    override fun execute(db: Database) {
        try {
            when (db) {
                is PostgresDatabase -> {
                    LOG.info("Executing BatchInsertChange on supported Database")
                    startMigration(db)
                }
                else -> {
                    LOG.info("Skipping BatchInsertChange due to non-supported Database")
                }
            }
        } catch (e: CustomChangeException) {
            throw e
        }
    }

    @Suppress("ThrowsCount", "NestedBlockDepth")
    private fun startMigration(db: PostgresDatabase) {
        if (db.connection == null || db.connection.isClosed) {
            LOG.severe("Not connected to database")
            throw CustomChangeException("Not connected to database")
        }

        val conn = db.connection as JdbcConnection
        // Fetch only the rows where not all values are synced yet
        val rs = conn.metaData.getBestRowIdentifier(
            fromCatalogName,
            fromSchemaName,
            fromTableName,
            DatabaseMetaData.bestRowSession,
            false
        )
        val columnNames = rs.getAll<String>(2).distinct().joinToString()
        val query = """
            INSERT INTO $toFullName
            SELECT * FROM $fromFullName
            WHERE $columnNames NOT IN (SELECT $columnNames FROM $toFullName)
            LIMIT $chunkSize;
        """.trimIndent()

        try {
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
            throw CustomChangeException("Could not insert records from $fromTableName into $toTableName in batch", e)
        } finally {
            stmt?.close()
        }
    }

    override fun rollback(database: Database?) {
        LOG.info("Rollback requested: no-op result")
    }

    companion object {
        const val DEFAULT_CHUNK_SIZE = 1000L
        const val DEFAULT_SLEEP_TIME = 0L
        val LOG: Logger = Scope.getCurrentScope().getLog(BulkTableCopyChange::class.java)
    }
}