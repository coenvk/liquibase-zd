package liquibase.ext.base

import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.exception.ValidationErrors
import liquibase.ext.change.constraint.SqlStatementWrapper
import liquibase.ext.util.KotlinExtensions.mapIf
import liquibase.sql.Sql
import liquibase.sqlgenerator.SqlGeneratorChain
import liquibase.sqlgenerator.SqlGeneratorFactory
import liquibase.sqlgenerator.core.AbstractSqlGenerator

abstract class RewriteSqlGenerator<T : SqlStatementWrapper> : AbstractSqlGenerator<T>() {
    protected fun generateOriginal(
        statement: T,
        database: Database
    ): Array<Sql> = getGeneratorFactory().generateSql(statement.original, database)

    protected fun Array<Sql>.rewriteSql(database: Database, rewrite: (Sql) -> Sql): Array<Sql> =
        mapIf(supportsRewrite(database)) {
            rewrite(it)
        }

    protected open fun supportsRewrite(database: Database): Boolean = database is PostgresDatabase

    override fun supports(statement: T, database: Database): Boolean =
        getGeneratorFactory().supports(statement.original, database)

    override fun validate(
        statement: T,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<T>
    ): ValidationErrors {
        val originalErrors = getGeneratorFactory().validate(statement.original, database)
        originalErrors.addAll(validateWrapper(statement, database, sqlGeneratorChain))
        return originalErrors
    }

    protected open fun validateWrapper(
        statement: T,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<T>
    ): ValidationErrors = ValidationErrors()

    private fun getGeneratorFactory(): SqlGeneratorFactory = SqlGeneratorFactory.getInstance()
}