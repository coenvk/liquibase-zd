package liquibase.ext.change.copy.table

import liquibase.change.*
import liquibase.change.core.DropTableChange
import liquibase.database.Database
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "copyTable",
    description = "Copy an existing table including the table structure, but excluding the data.",
    priority = ChangeMetaData.PRIORITY_DEFAULT,
    appliesTo = ["table"]
)
class CopyTableChange : AbstractTableChange() {
    var copyTableName: String? = null
        @DatabaseChangeProperty(
            description = "Name of the table to copy to",
            requiredForDatabase = [ChangeParameterMetaData.ALL]
        )
        get

    override fun getConfirmationMessage(): String = "Table $tableName has been copied to $copyTableName"

    override fun generateStatements(database: Database): Array<SqlStatement> {
        return arrayOf(CopyTableStatement(catalogName, schemaName, tableName, copyTableName!!))
    }

    override fun createInverses(): Array<Change> = arrayOf(
        DropTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = copyTableName
        }
    )
}