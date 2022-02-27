package liquibase.ext.change.custom

import liquibase.change.custom.CustomTaskRollback
import liquibase.database.Database
import liquibase.statement.AbstractSqlStatement

class CustomTaskRollbackStatement(private val customRollback: CustomTaskRollback) : AbstractSqlStatement() {
    fun rollback(database: Database) = customRollback.rollback(database)
}