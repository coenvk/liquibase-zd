package liquibase.ext.change.drop.trigger

import liquibase.change.core.RawSQLChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase

class DropSyncTriggerChange(
    var triggerName: String,
    var tableName: String
) : RawSQLChange() {
    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    init {
        this.dbms = "postgresql"
        this.sql = """DROP TRIGGER IF EXISTS $triggerName ON $tableName;"""
    }
}