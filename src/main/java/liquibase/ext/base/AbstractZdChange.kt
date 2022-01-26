package liquibase.ext.base

import liquibase.change.AbstractChange
import liquibase.change.Change
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.ext.util.ChangeUtils
import liquibase.statement.SqlStatement

abstract class AbstractZdChange<T : Change>(
    private val originalChange: T,
    private val generateOriginalStatements: (Database) -> Array<SqlStatement>
) : ZdChange<T>, AbstractChange() {
    private var expandChanges: Array<Change> = emptyArray()
    private var contractChanges: Array<Change> = emptyArray()

    override fun getConfirmationMessage(): String = originalChange.confirmationMessage

    override fun supports(database: Database): Boolean =
        originalChange.supports(database) && database is PostgresDatabase

    final override fun generateStatements(database: Database): Array<SqlStatement> {
        val mode = ChangeUtils.getMode(originalChange)
        if (mode == ZdMode.EXPAND) {
            return generateExpandStatements(database)
        } else if (mode == ZdMode.CONTRACT) {
            return generateContractStatements(database)
        }
        return generateOriginalStatements(database)
    }

    final override fun generateExpandStatements(database: Database): Array<SqlStatement> {
        if (expandChanges.isEmpty()) expandChanges = generateExpandChanges(database)
        return expandChanges.flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    final override fun generateContractStatements(database: Database): Array<SqlStatement> {
        if (contractChanges.isEmpty()) contractChanges = generateContractChanges(database)
        return contractChanges.flatMap { it.generateStatements(database).asList() }.toTypedArray()
    }

    public final override fun createInverses(): Array<Change>? {
        val mode = ChangeUtils.getMode(originalChange)
        if (mode == ZdMode.EXPAND) {
            return createExpandInverses()
        } else if (mode == ZdMode.OFF) {
            return originalChange.invokeHiddenMethod("createInverses") as Array<Change>?
        }
        return null
    }
}