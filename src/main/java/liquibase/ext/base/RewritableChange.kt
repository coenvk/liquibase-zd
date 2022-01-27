package liquibase.ext.base

import liquibase.change.Change
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.statement.SqlStatement

interface RewritableChange : Change {
    fun Array<SqlStatement>.orElse(
        database: Database,
        generateOthers: (Database) -> Array<SqlStatement>
    ): Array<SqlStatement> = if (isEmpty()) {
        generateOthers(database)
    } else this

    private fun Database.isRequiredPostgresVersion(): Boolean = this is PostgresDatabase
    private fun supportsZd(database: Database): Boolean = database.isRequiredPostgresVersion()
}