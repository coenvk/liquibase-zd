package liquibase.ext.change.rename.view

import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.CreateViewChange
import liquibase.change.core.DropViewChange
import liquibase.change.core.RenameViewChange
import liquibase.database.Database
import liquibase.ext.base.ZdChange
import liquibase.ext.metadata.view.ViewCopyTask
import liquibase.ext.metadata.view.ViewMetadata
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "renameView",
    description = "Renames an existing view.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["view"]
)
class ZdRenameViewChange : RenameViewChange(), ZdChange {
    override fun generateStatements(database: Database): Array<SqlStatement> =
        generateZdStatements(database) { super.generateStatements(it) }

    override fun createInverses(): Array<Change>? =
        createZdInverses { super.createInverses() }

    override fun generateStatementsVolatile(database: Database): Boolean = isExpand(database)

    override fun generateExpandChanges(database: Database): Array<Change> {
        val viewMetadata = ViewCopyTask().copy(
            database, ViewMetadata(), mapOf(
                "viewName" to oldViewName
            )
        )
        return arrayOf(
            CreateViewChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.viewName = newViewName
                it.selectQuery = viewMetadata.definition
            }
        )
    }

    override fun generateContractChanges(database: Database): Array<Change> {
        return arrayOf(
            DropViewChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.viewName = oldViewName
            }
        )
    }

    override fun createExpandInverses(): Array<Change> {
        return arrayOf(
            DropViewChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.viewName = newViewName
            }
        )
    }
}