package liquibase.ext.change.rename.column

import liquibase.Scope
import liquibase.change.AddColumnConfig
import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.*
import liquibase.database.Database
import liquibase.exception.UnexpectedLiquibaseException
import liquibase.ext.base.ZdChange
import liquibase.ext.change.constraint.AddForeignKeyNotValidChange
import liquibase.ext.change.custom.CustomChangeDecorator
import liquibase.ext.change.internal.create.trigger.syncInsertTriggerChange
import liquibase.ext.change.internal.create.trigger.syncUpdateTriggerChange
import liquibase.ext.change.internal.drop.trigger.DropSyncTriggerChange
import liquibase.ext.change.update.BulkColumnCopyChange
import liquibase.ext.metadata.column.ColumnCopyTask
import liquibase.ext.metadata.column.ColumnMetadata
import liquibase.ext.metadata.column.PrimaryKeyConstraintMetadata
import liquibase.logging.Logger
import liquibase.statement.SqlStatement
import liquibase.structure.core.Column

@DatabaseChange(
    name = "renameColumn",
    description = "Renames an existing column.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["column"]
)
class ZdRenameColumnChange : RenameColumnChange(), ZdChange {
    var batchChunkSize: Long? = BulkColumnCopyChange.DEFAULT_CHUNK_SIZE
    var batchSleepTime: Long? = BulkColumnCopyChange.DEFAULT_SLEEP_TIME

    override fun generateStatements(database: Database): Array<SqlStatement> =
        generateZdStatements(database) { super.generateStatements(it) }

    override fun createInverses(): Array<Change>? =
        createZdInverses { super.createInverses() }

    override fun generateStatementsVolatile(database: Database): Boolean = isZdEnabled(database)
    override fun generateRollbackStatementsVolatile(database: Database): Boolean = isRollbackZdEnabled(database)

    override fun generateExpandChanges(database: Database): Array<Change> {
        val columnMetadata = ColumnCopyTask().copy(
            database, ColumnMetadata(), mapOf(
                "tableName" to tableName,
                "columnName" to oldColumnName
            )
        )
        if (columnMetadata.constraints.filterIsInstance<PrimaryKeyConstraintMetadata>().any()) {
            throw UnexpectedLiquibaseException("Current implementation does not support renaming a primary key column.")
        }
        val constraintChanges = columnMetadata.constraints.map {
            it.toChange(
                catalogName,
                schemaName,
                tableName,
                oldColumnName,
                newColumnName
            )
        }.toTypedArray()
        return arrayOf(
            AddColumnChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                val newColumn = Column.fromName(newColumnName).setNullable(true)
                val newColumnConfig = AddColumnConfig(newColumn)
                newColumnConfig.type = columnMetadata.type
                it.columns = listOf(newColumnConfig)
            },
            *constraintChanges.filterNot { it is AddForeignKeyNotValidChange }.toTypedArray(),
            *syncUpdateTriggerChange(
                catalogName,
                schemaName,
                tableName,
                "t1",
                oldColumnName,
                newColumnName,
            ),
            *syncInsertTriggerChange(
                catalogName,
                schemaName,
                tableName,
                "t2",
                oldColumnName,
                newColumnName,
            ),
            CustomChangeDecorator().setClass(BulkColumnCopyChange::class.java.name).also {
                it.setParam("catalogName", catalogName)
                it.setParam("schemaName", schemaName)
                it.setParam("tableName", tableName)
                it.setParam("fromColumns", oldColumnName)
                it.setParam("toColumns", newColumnName)
                it.setParam("rowId", columnMetadata.primaryKeyOrNull())
                it.setParam("chunkSize", batchChunkSize?.toString())
                it.setParam("sleepTime", batchSleepTime?.toString())
            },
            *constraintChanges.filterIsInstance<AddForeignKeyNotValidChange>().toTypedArray(),
            if (columnMetadata.isNullable) EmptyChange()
            else {
                AddNotNullConstraintChange().also {
                    it.catalogName = catalogName
                    it.schemaName = schemaName
                    it.tableName = tableName
                    it.columnName = newColumnName
                    it.columnDataType = columnMetadata.type
                }
            },
            if (columnMetadata.defaultValue == null) EmptyChange()
            else {
                AddDefaultValueChange().also {
                    it.catalogName = catalogName
                    it.schemaName = schemaName
                    it.tableName = tableName
                    it.columnName = newColumnName
                    it.columnDataType = columnMetadata.type
                }
            },
            *syncUpdateTriggerChange(
                catalogName,
                schemaName,
                tableName,
                "t3",
                newColumnName,
                oldColumnName,
            )
        )
    }

    override fun generateContractChanges(database: Database): Array<Change> {
        val columnMetadata = ColumnCopyTask().copy(
            database, ColumnMetadata(), mapOf(
                "tableName" to tableName,
                "columnName" to oldColumnName
            )
        )
        val constraintChanges = columnMetadata.constraints.map {
            it.toRollback(
                catalogName,
                schemaName,
                tableName
            )
        }.toTypedArray()
        return arrayOf(
            DropSyncTriggerChange(
                "t1",
                tableName
            ),
            DropSyncTriggerChange(
                "t2",
                tableName
            ),
            DropSyncTriggerChange(
                "t3",
                tableName
            ),
            *constraintChanges,
            DropColumnChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = oldColumnName
            }
        )
    }

    override fun createExpandInverses(): Array<Change> {
        val columnMetadata = ColumnCopyTask().copy(
            Scope.getCurrentScope().database, ColumnMetadata(), mapOf(
                "tableName" to tableName,
                "columnName" to newColumnName
            )
        )
        val constraintChanges = columnMetadata.constraints.map {
            it.toRollback(
                catalogName,
                schemaName,
                tableName
            )
        }.toTypedArray()
        return arrayOf(
            DropSyncTriggerChange(
                "t3",
                tableName
            ),
            DropNotNullConstraintChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = newColumnName
            },
            DropSyncTriggerChange(
                "t2",
                tableName
            ),
            DropSyncTriggerChange(
                "t1",
                tableName
            ),
            *constraintChanges,
            DropColumnChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = newColumnName
            }
        )
    }

    companion object {
        private val LOG: Logger = Scope.getCurrentScope().getLog(ZdRenameColumnChange::class.java)
    }
}