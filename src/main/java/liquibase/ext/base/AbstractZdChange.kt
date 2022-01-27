package liquibase.ext.base

import liquibase.change.AbstractChange
import liquibase.change.Change
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.ext.util.ChangeUtils
import liquibase.statement.SqlStatement

abstract class AbstractZdChange<T : Change>(
    private val originalChange: T
) : AbstractChange(), ZdChange<T> {
    private var expandChanges: Array<Change> = emptyArray()
    private var contractChanges: Array<Change> = emptyArray()

    override fun getConfirmationMessage(): String = originalChange.confirmationMessage

    private fun Database.isRequiredPostgresVersion(): Boolean = this is PostgresDatabase
    private fun supportsZd(database: Database): Boolean = database.isRequiredPostgresVersion()

    final override fun generateStatements(database: Database): Array<SqlStatement> =
        if (!supportsZd(database)) emptyArray() else when (ChangeUtils.getMode(originalChange)) {
            ZdMode.EXPAND -> generateExpandStatements(database)
            ZdMode.CONTRACT -> generateContractStatements(database)
            else -> emptyArray()
        }

    final override fun generateExpandStatements(database: Database): Array<SqlStatement> {
        if (expandChanges.isEmpty()) expandChanges = generateExpandChanges(database)
        return expandChanges.flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    final override fun generateContractStatements(database: Database): Array<SqlStatement> {
        if (contractChanges.isEmpty()) contractChanges = generateContractChanges(database)
        return contractChanges.flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    final override fun generateRollbackStatements(database: Database): Array<SqlStatement> {
        return if (!supportsZd(database)) emptyArray() else super.generateRollbackStatements(database)
    }

    final override fun createInverses(): Array<Change>? {
        return when (ChangeUtils.getMode(originalChange)) {
            ZdMode.EXPAND -> createExpandInverses()
            else -> null
        }
    }
}