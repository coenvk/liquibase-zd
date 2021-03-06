package liquibase.ext.change.custom

import liquibase.Scope
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.custom.CustomChangeWrapper
import liquibase.change.custom.CustomTaskChange
import liquibase.change.custom.CustomTaskRollback
import liquibase.database.Database
import liquibase.exception.CustomChangeException
import liquibase.exception.UnexpectedLiquibaseException
import liquibase.logging.Logger
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "customChange",
    description = """Although Liquibase tries to provide a wide range of database refactorings, there are times you may want to create your own custom refactoring class.

To create your own custom refactoring, simply create a class that implements the liquibase.change.custom.CustomSqlChange or liquibase.change.custom.CustomTaskChange interface and use the <custom> tag in your change set.

If your change can be rolled back, implement the liquibase.change.custom.CustomSqlRollback interface as well.

For a sample custom change class, see liquibase.change.custom.ExampleCustomSqlChange""",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1
)
class CustomChangeDecorator : CustomChangeWrapper() {
    override fun generateStatements(database: Database): Array<SqlStatement> {
        try {
            confirmationMessage // calls private method configureCustomChange
            if (customChange is CustomTaskChange) {
                LOG.info("CustomTaskChange will be executed in sql generator.")
                return arrayOf(CustomTaskStatement(this))
            }
            return super.generateStatements(database)
        } catch (e: CustomChangeException) {
            throw UnexpectedLiquibaseException(e)
        }
    }

    override fun generateRollbackStatements(database: Database): Array<SqlStatement> {
        try {
            confirmationMessage // calls private method configureCustomChange
            if (customChange is CustomTaskRollback) {
                LOG.info("CustomTaskRollback will be executed in sql generator.")
                return arrayOf(CustomTaskRollbackStatement(this))
            }
            return super.generateRollbackStatements(database)
        } catch (e: CustomChangeException) {
            throw UnexpectedLiquibaseException(e)
        }
    }

    companion object {
        private val LOG: Logger = Scope.getCurrentScope().getLog(CustomChangeDecorator::class.java)
    }
}