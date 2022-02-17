package liquibase.ext.change.rename.table

import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.CreateViewChange
import liquibase.change.core.DropViewChange
import liquibase.change.core.RenameTableChange
import liquibase.database.Database
import liquibase.ext.base.ZdChange
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "renameTableUsingView",
    description = "Renames an existing table.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["table"]
)
class ZdRenameTableUsingViewChange : RenameTableChange(), ZdChange {
    override fun generateStatements(database: Database): Array<SqlStatement> =
        generateZdStatements(database) { super.generateStatements(it) }

    override fun createInverses(): Array<Change>? =
        createZdInverses { super.createInverses() }

    override fun generateExpandChanges(database: Database): Array<Change> = arrayOf(
        RenameTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.oldTableName = oldTableName
            it.newTableName = newTableName
        },
        CreateViewChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.viewName = oldTableName
            it.selectQuery = "SELECT * FROM ${if (schemaName == null) "" else "${schemaName}."}$newTableName"
        }
    )

    override fun generateContractChanges(database: Database): Array<Change> = arrayOf(
        DropViewChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.viewName = oldTableName
        }
    )

    override fun createExpandInverses(): Array<Change> = arrayOf(
        DropViewChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.viewName = oldTableName
        },
        RenameTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.oldTableName = newTableName
            it.newTableName = oldTableName
        }
    )
}