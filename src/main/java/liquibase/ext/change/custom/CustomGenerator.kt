package liquibase.ext.change.custom

import liquibase.Scope
import liquibase.database.Database
import liquibase.exception.CustomChangeException
import liquibase.exception.UnexpectedLiquibaseException
import liquibase.exception.ValidationErrors
import liquibase.executor.ExecutorService
import liquibase.executor.LoggingExecutor
import liquibase.logging.Logger
import liquibase.sql.SingleLineComment
import liquibase.sql.Sql
import liquibase.sqlgenerator.SqlGeneratorChain
import liquibase.sqlgenerator.core.AbstractSqlGenerator
import java.util.*

abstract class CustomGenerator<T : CustomStatement> : AbstractSqlGenerator<T>() {
    override fun validate(
        statement: T,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<T>
    ): ValidationErrors {
        val validationErrors = ValidationErrors()
        validationErrors.checkRequiredField("customChangeWrapper", statement.customChangeWrapper)
        return validationErrors
    }

    override fun generateSql(
        statement: T,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<T>
    ): Array<Sql> {
        try {
            val isVolatile = when (statement) {
                is CustomTaskStatement -> statement.customChangeWrapper.generateStatementsVolatile(database)
                is CustomTaskRollbackStatement -> {
                    statement.customChangeWrapper.generateRollbackStatementsVolatile(database)
                }
                else -> false
            }
            if (isVolatile && !isLoggingExecutor(database)
            ) {
                val startTime = System.currentTimeMillis()
                LOG.info("Start execution of custom task.")
                statement.execute(database)
                LOG.info("Finished execution of custom task in ${System.currentTimeMillis() - startTime}ms")
            }
        } catch (e: CustomChangeException) {
            throw UnexpectedLiquibaseException(e)
        }
        return arrayOf(
            SingleLineComment(
                "Executed a custom change operation: ${statement.customChangeWrapper.customChange::class.java.simpleName}",
                database.lineComment
            )
        )
    }

    private fun isLoggingExecutor(database: Database): Boolean {
        val executorService = Scope.getCurrentScope().getSingleton(
            ExecutorService::class.java
        )
        return executorService.executorExists("logging", database) &&
                executorService.getExecutor("logging", database) is LoggingExecutor
    }

    companion object {
        private val LOG: Logger = Scope.getCurrentScope().getLog(CustomGenerator::class.java)
    }
}

class CustomTaskGenerator : CustomGenerator<CustomTaskStatement>()
class CustomTaskRollbackGenerator : CustomGenerator<CustomTaskRollbackStatement>()