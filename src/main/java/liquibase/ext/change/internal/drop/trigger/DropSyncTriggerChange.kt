package liquibase.ext.change.internal.drop.trigger

import liquibase.change.core.RawSQLChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase

internal class DropSyncTriggerChange(
    triggerName: String,
    tableName: String
) : RawSQLChange() {
    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    init {
        this.dbms = "postgresql"
        this.sql = """DROP TRIGGER IF EXISTS $triggerName ON $tableName;"""
    }
}