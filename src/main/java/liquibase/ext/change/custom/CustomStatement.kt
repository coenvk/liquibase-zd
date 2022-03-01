package liquibase.ext.change.custom

import liquibase.change.custom.CustomChangeWrapper
import liquibase.change.custom.CustomTaskChange
import liquibase.change.custom.CustomTaskRollback
import liquibase.database.Database
import liquibase.exception.CustomChangeException
import liquibase.statement.AbstractSqlStatement

sealed class CustomStatement(
    internal val customChangeWrapper: CustomChangeWrapper
) : AbstractSqlStatement() {
    abstract fun execute(database: Database)
}

class CustomTaskStatement(
    customChangeWrapper: CustomChangeWrapper
) : CustomStatement(customChangeWrapper) {
    override fun execute(database: Database) {
        try {
            (customChangeWrapper.customChange as CustomTaskChange).execute(database)
        } catch (e: ClassCastException) {
            throw CustomChangeException("Statement can only be used with a CustomTaskChange.", e)
        }
    }
}

class CustomTaskRollbackStatement(
    customChangeWrapper: CustomChangeWrapper
) : CustomStatement(customChangeWrapper) {
    override fun execute(database: Database) {
        try {
            (customChangeWrapper.customChange as CustomTaskRollback).rollback(database)
        } catch (e: ClassCastException) {
            throw CustomChangeException("Statement can only be used with a CustomTaskRollback.", e)
        }
    }
}