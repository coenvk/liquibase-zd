package liquibase.ext.change.rename.table

import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.RenameTableChange
import liquibase.database.Database
import liquibase.ext.base.ZdChange

@DatabaseChange(
    name = "renameTable",
    description = "Renames an existing table",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["table"]
)
class ZdRenameTableChange : RenameTableChange(), ZdChange {
//    override fun generateStatements(database: Database): Array<SqlStatement> =
//        generateZdStatements(database) { super.generateStatements(it) }
//
//    override fun createInverses(): Array<Change>? =
//        createZdInverses { super.createInverses() }

    override fun generateExpandChanges(database: Database): Array<Change> {
        TODO("Not yet implemented")
    }

    override fun generateContractChanges(database: Database): Array<Change> {
        TODO("Not yet implemented")
    }

    override fun createExpandInverses(): Array<Change> {
        TODO("Not yet implemented")
    }
}