package liquibase.ext.change.rename.column

import liquibase.change.AddColumnConfig
import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.*
import liquibase.database.Database
import liquibase.ext.base.AbstractZdChange
import liquibase.ext.base.RewritableChange
import liquibase.ext.change.create.trigger.syncInsertTriggerChange
import liquibase.ext.change.create.trigger.syncUpdateTriggerChange
import liquibase.ext.change.drop.trigger.DropSyncTriggerChange
import liquibase.statement.SqlStatement
import liquibase.structure.core.Column

@DatabaseChange(
    name = "renameColumn",
    description = "Renames an existing column.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["column"]
)
class ZdRenameColumnChange : RenameColumnChange(), RewritableChange {
    private val internalChange = InternalZdRenameColumnChange()

    override fun generateRollbackStatements(database: Database): Array<SqlStatement> =
        internalChange.generateRollbackStatements(database).orElse(database) { super.generateRollbackStatements(it) }

    override fun generateStatements(database: Database): Array<SqlStatement> =
        internalChange.generateStatements(database).orElse(database) { super.generateStatements(it) }

    internal inner class InternalZdRenameColumnChange :
        AbstractZdChange<RenameColumnChange>(
            this@ZdRenameColumnChange
        ) {
        override fun generateExpandChanges(database: Database): Array<Change> = arrayOf(
            AddColumnChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                val newColumn = Column.fromName(newColumnName).setNullable(true)
                val newColumnConfig = AddColumnConfig(newColumn)
                newColumnConfig.type = columnDataType
                it.columns = listOf(newColumnConfig)
            },
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
            RawSQLChange().also {
                it.sql =
                    """UPDATE $tableName SET $newColumnName = ${oldColumnName};"""
            },
            AddNotNullConstraintChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = newColumnName
                it.columnDataType = columnDataType
                it.validate = false
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

        override fun generateContractChanges(database: Database): Array<Change> = arrayOf(
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
            DropColumnChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = oldColumnName
            }
        )

        override fun createExpandInverses(): Array<Change> = arrayOf(
            DropSyncTriggerChange(
                "t3",
                tableName
            ),
            DropNotNullConstraintChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = newColumnName
                it.columnDataType = columnDataType
            },
            DropSyncTriggerChange(
                "t2",
                tableName
            ),
            DropSyncTriggerChange(
                "t1",
                tableName
            ),
            DropColumnChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = newColumnName
            }
        )
    }
}