package liquibase.ext.base

import liquibase.change.Change
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.ext.util.KotlinExtensions.mapIf
import liquibase.statement.SqlStatement

interface RewritableChange : Change {
    fun Array<SqlStatement>.rewriteStatements(
        database: Database,
        rewrite: (SqlStatement) -> SqlStatement
    ): Array<SqlStatement> = mapIf(supportsRewrite(database)) {
        rewrite(it)
    }

    fun supportsRewrite(database: Database): Boolean = database is PostgresDatabase
}