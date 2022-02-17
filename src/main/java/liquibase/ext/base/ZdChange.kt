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
        if (!supportsZd(database)) generateOriginal(database) else when (getStrategy()) {
            ZdStrategy.EXPAND -> generateExpandStatements(database)
            ZdStrategy.CONTRACT -> generateContractStatements(database)
            else -> generateOriginal(database)
        }

    fun createZdInverses(createOriginalInverses: () -> Array<Change>?): Array<Change>? =
        if (!supportsZd(Scope.getCurrentScope().database)) {
            createOriginalInverses()
        } else when (getStrategy()) {
            ZdStrategy.EXPAND -> createExpandInverses()
            ZdStrategy.CONTRACT -> null
            else -> createOriginalInverses()
        }

    fun getStrategy(): ZdStrategy {
        val changeSet = changeSet ?: return DEFAULT_STRATEGY
        val strategy = (changeSet.changeLogParameters.getValue(
            PROPERTY_KEY_ZD_STRATEGY,
            changeSet.changeLog
        ) ?: DEFAULT_STRATEGY).toString()
        return Arrays
            .stream(ZdStrategy.values())
            .filter { m: ZdStrategy -> m.name.lowercase() == strategy.lowercase() }
            .findAny()
            .orElse(DEFAULT_STRATEGY)
    }

    fun isZdEnabled(database: Database): Boolean = supportsZd(database) && getStrategy() != ZdStrategy.OFF
    fun isRollbackZdEnabled(database: Database): Boolean = supportsZd(database) && getStrategy() == ZdStrategy.EXPAND

    private fun generateExpandStatements(database: Database): Array<SqlStatement> {
        return generateExpandChanges(database).flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    private fun generateContractStatements(database: Database): Array<SqlStatement> {
        return generateContractChanges(database).flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    private fun supportsZd(database: Database?): Boolean = database is PostgresDatabase

    companion object {
        const val PROPERTY_KEY_ZD_STRATEGY = "zd-strategy"
        private val DEFAULT_STRATEGY = ZdStrategy.OFF
    }
}