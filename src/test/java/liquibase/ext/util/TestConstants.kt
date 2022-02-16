package liquibase.ext.util

import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import liquibase.Scope
import liquibase.change.Change
import liquibase.change.core.RenameColumnChange
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.database.core.PostgresDatabase
import liquibase.ext.base.ZdChange
import liquibase.ext.base.ZdStrategy
import liquibase.ext.change.rename.column.ZdRenameColumnChange

object TestConstants {
    private val dataTypeArb = Arb.of("bigint", "boolean", "int", "float", "decimal", "double", "char", "clob")
    private val alphaNumArb = Arb.stringPattern("[a-zA-Z0-9]+")

    private val renameColumnPairArb =
        Arb.bind(
            alphaNumArb.orNull(),
            alphaNumArb,
            alphaNumArb,
            alphaNumArb,
            dataTypeArb
        ) { schema, table, oldColumn, newColumn, dataType ->
            ZdRenameColumnChange().apply {
                schemaName = schema
                tableName = table
                oldColumnName = oldColumn
                newColumnName = newColumn
                columnDataType = dataType
            } to RenameColumnChange().apply {
                schemaName = schema
                tableName = table
                oldColumnName = oldColumn
                newColumnName = newColumn
                columnDataType = dataType
            }
        }

    private fun Change.createChangeSet(zdStrategy: ZdStrategy = ZdStrategy.OFF): ChangeSet = run {
        val changeLog = mockk<DatabaseChangeLog>(relaxed = true)
        spyk(ChangeSet(changeLog)).apply {
            every { changes } returns listOf(this@createChangeSet)
            val params = ChangeLogParameters()
            params.set(ZdChange.PROPERTY_KEY_ZD_STRATEGY, zdStrategy.toString())
            every { changeLogParameters } returns params
        }
    }

    private fun Pair<Change, Change>.addContext(zdStrategy: ZdStrategy = ZdStrategy.OFF) =
        first.apply { changeSet = createChangeSet(zdStrategy) } to
                second.apply { changeSet = createChangeSet(zdStrategy) }

    fun Database.runInScope(scopedRunner: () -> Unit) {
        val scopeObjects: MutableMap<String, Any> = HashMap()
        scopeObjects[Scope.Attr.database.name] = this
        Scope.child(scopeObjects, scopedRunner)
    }

    private val zdChangeArb = Arb.choice(renameColumnPairArb)

    val zdOffChangeArb = zdChangeArb.map { it.addContext() }
    val zdExpandChangeArb = zdChangeArb.map { it.addContext(ZdStrategy.EXPAND) }
    val zdContractChangeArb = zdChangeArb.map { it.addContext(ZdStrategy.CONTRACT) }

    val supportedDatabases = Arb.of(DatabaseFactory.getInstance().implementedDatabases
        .filterIsInstance<PostgresDatabase>())
    val unsupportedDatabases = Arb.of(DatabaseFactory.getInstance().implementedDatabases
        .filterNot { it is PostgresDatabase })
}