package liquibase.ext.change.custom

import liquibase.change.custom.CustomTaskChange
import liquibase.database.Database
import liquibase.statement.AbstractSqlStatement

class CustomTaskStatement(private val customChange: CustomTaskChange) : AbstractSqlStatement() {
    fun execute(database: Database) = customChange.execute(database)
}