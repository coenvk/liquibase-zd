package liquibase.ext.base

import liquibase.change.Change
import liquibase.database.Database
import liquibase.statement.SqlStatement

internal interface ZdChange<out T : Change> : Change {
    fun generateExpandChanges(database: Database): Array<Change>
    fun generateContractChanges(database: Database): Array<Change>
    fun createExpandInverses(): Array<Change>

    fun generateExpandStatements(database: Database): Array<SqlStatement>
    fun generateContractStatements(database: Database): Array<SqlStatement>
}