package liquibase.ext.change.rename.table

import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.DropTableChange
import liquibase.change.core.RenameTableChange
import liquibase.database.Database
import liquibase.ext.base.ZdChange
import liquibase.ext.change.copy.table.CopyTableChange
import liquibase.ext.change.custom.CustomChangeDecorator
import liquibase.ext.change.update.BulkTableCopyChange
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "renameTableUsingCopy",
    description = "Renames an existing table.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["table"]
)
class ZdRenameTableUsingCopyChange : RenameTableChange(), ZdChange {
    override fun generateStatements(database: Database): Array<SqlStatement> =
        generateZdStatements(database) { super.generateStatements(it) }

    override fun createInverses(): Array<Change>? =
        createZdInverses { super.createInverses() }

    override fun generateExpandChanges(database: Database): Array<Change> = arrayOf(
        CopyTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = oldTableName
            it.copyTableName = newTableName
        },
        CustomChangeDecorator().setClass(BulkTableCopyChange::class.java.name).also {
            it.setParam("fromCatalogName", catalogName)
            it.setParam("toCatalogName", catalogName)
            it.setParam("fromSchemaName", schemaName)
            it.setParam("toSchemaName", schemaName)
            it.setParam("fromTableName", oldTableName)
            it.setParam("toTableName", newTableName)
        }
    )

    override fun generateContractChanges(database: Database): Array<Change> = arrayOf(
        DropTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = oldTableName
            it.isCascadeConstraints = true
        }
    )

    override fun createExpandInverses(): Array<Change> = arrayOf(
        DropTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = newTableName
            it.isCascadeConstraints = true
        }
    )
}