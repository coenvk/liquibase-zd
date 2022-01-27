package liquibase.ext.base

import liquibase.Scope
import liquibase.change.Change
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.ext.util.ChangeUtils
import liquibase.statement.SqlStatement

interface ZdChange : Change {
    fun generateExpandChanges(database: Database): Array<Change>
    fun generateContractChanges(database: Database): Array<Change>
    fun createExpandInverses(): Array<Change>

    fun generateZdStatements(
        database: Database,
        generateOriginal: (Database) -> Array<SqlStatement>
    ): Array<SqlStatement> =
        if (!supportsZd(database)) generateOriginal(database) else when (ChangeUtils.getMode(this)) {
            ZdMode.EXPAND -> generateExpandStatements(database)
            ZdMode.CONTRACT -> generateContractStatements(database)
            else -> generateOriginal(database)
        }

    fun createZdInverses(createOriginalInverses: () -> Array<Change>?): Array<Change>? =
        if (!supportsZd(Scope.getCurrentScope().database)) createOriginalInverses() else when (ChangeUtils.getMode(
            this
        )) {
            ZdMode.EXPAND -> createExpandInverses()
            ZdMode.CONTRACT -> null
            else -> createOriginalInverses()
        }

    private fun generateExpandStatements(database: Database): Array<SqlStatement> {
        return generateExpandChanges(database).flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    private fun generateContractStatements(database: Database): Array<SqlStatement> {
        return generateContractChanges(database).flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    private fun Database.isRequiredPostgresVersion(): Boolean = this is PostgresDatabase
    private fun supportsZd(database: Database): Boolean = database.isRequiredPostgresVersion()
}