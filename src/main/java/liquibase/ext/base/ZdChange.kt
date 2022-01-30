package liquibase.ext.base

import liquibase.Scope
import liquibase.change.Change
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.statement.SqlStatement
import java.util.*

interface ZdChange : Change {
    fun generateExpandChanges(database: Database): Array<Change>
    fun generateContractChanges(database: Database): Array<Change>
    fun createExpandInverses(): Array<Change>

    fun generateZdStatements(
        database: Database,
        generateOriginal: (Database) -> Array<SqlStatement>
    ): Array<SqlStatement> =
        if (!supportsZd(database)) generateOriginal(database) else when (getMode()) {
            ZdMode.EXPAND -> generateExpandStatements(database)
            ZdMode.CONTRACT -> generateContractStatements(database)
            else -> generateOriginal(database)
        }

    fun createZdInverses(createOriginalInverses: () -> Array<Change>?): Array<Change>? =
        if (!supportsZd(Scope.getCurrentScope().database)) {
            createOriginalInverses()
        } else when (getMode()) {
            ZdMode.EXPAND -> createExpandInverses()
            ZdMode.CONTRACT -> null
            else -> createOriginalInverses()
        }

    fun getMode(): ZdMode {
        val changeSet = changeSet ?: return DEFAULT_MODE
        val modeName = (changeSet.changeLogParameters.getValue(
            PROPERTY_KEY_ZD_MODE,
            changeSet.changeLog
        ) ?: DEFAULT_MODE).toString()
        return Arrays
            .stream(ZdMode.values())
            .filter { m: ZdMode -> m.name.lowercase() == modeName.lowercase() }
            .findAny()
            .orElse(DEFAULT_MODE)
    }

    private fun generateExpandStatements(database: Database): Array<SqlStatement> {
        return generateExpandChanges(database).flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    private fun generateContractStatements(database: Database): Array<SqlStatement> {
        return generateContractChanges(database).flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    private fun supportsZd(database: Database): Boolean = database is PostgresDatabase

    companion object {
        const val PROPERTY_KEY_ZD_MODE = "zd-mode"
        private val DEFAULT_MODE = ZdMode.OFF
    }
}