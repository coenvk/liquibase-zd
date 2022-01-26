package liquibase.ext.base

import liquibase.change.custom.CustomSqlChange
import liquibase.database.Database
import liquibase.exception.CustomChangeException
import liquibase.exception.SetupException
import liquibase.exception.ValidationErrors
import liquibase.resource.ResourceAccessor
import liquibase.statement.SqlStatement
import liquibase.statement.core.RawSqlStatement

class BatchMigrationChange : CustomSqlChange {
    private val tableName: String? = null
    private val fromColumns: String? = null
    private val toColumns: String? = null
    private val chunkSize: String? = null

    @Throws(CustomChangeException::class)
    override fun generateStatements(database: Database): Array<SqlStatement> {
        val sqlStatement = RawSqlStatement(
            """
            UPDATE $tableName
            SET $toColumns = data.$fromColumns
            WHERE ctid IN (SELECT ctid
                            FROM $tableName
                            WHERE $toColumns IS NULL
                            LIMIT $chunkSize);
            """
        )
        return arrayOf(sqlStatement)
    }

    override fun getConfirmationMessage(): String {
        return ("Batch migration change on table " + tableName + " from columns " + fromColumns + " to " + toColumns
                + " has been executed successfully.")
    }

    @Throws(SetupException::class)
    override fun setUp() {
    }

    override fun setFileOpener(resourceAccessor: ResourceAccessor) {}
    override fun validate(database: Database): ValidationErrors? {
        return null
    }
}