package liquibase.ext.change.rename.column

import liquibase.change.AddColumnConfig
import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.*
import liquibase.database.Database
import liquibase.ext.base.ZdChange
import liquibase.ext.change.internal.create.trigger.syncInsertTriggerChange
import liquibase.ext.change.internal.create.trigger.syncUpdateTriggerChange
import liquibase.ext.change.internal.drop.trigger.DropSyncTriggerChange
import liquibase.ext.metadata.ColumnCopyTask
import liquibase.ext.metadata.ColumnMetadata
import liquibase.statement.SqlStatement
import liquibase.structure.core.Column

@DatabaseChange(
    name = "renameColumn",
    description = "Renames an existing column.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["column"]
)
class ZdRenameColumnChange : RenameColumnChange(), ZdChange {
    override fun generateStatements(database: Database): Array<SqlStatement> =
        generateZdStatements(database) { super.generateStatements(it) }

    override fun createInverses(): Array<Change>? =
        createZdInverses { super.createInverses() }

    override fun generateStatementsVolatile(database: Database): Boolean = true

    override fun generateExpandChanges(database: Database): Array<Change> {
        val columnMetadata = ColumnCopyTask().copy(
            database, ColumnMetadata(newColumnName), mapOf(
                "tableName" to tableName,
                "columnName" to oldColumnName
            )
        )
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
            *constraintChanges,
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
//            CustomChangeWrapper().setClass(BatchMigrationChange::class.java.name).also {
//                it.setParam("catalogName", catalogName)
//                it.setParam("schemaName", schemaName)
//                it.setParam("tableName", tableName)
//                it.setParam("fromColumns", oldColumnName)
//                it.setParam("toColumns", newColumnName)
//            },
            RawSQLChange().also {
                it.sql =
                    """UPDATE $tableName SET $newColumnName = $oldColumnName WHERE $newColumnName IS NULL AND $oldColumnName IS NOT NULL;"""
            },
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