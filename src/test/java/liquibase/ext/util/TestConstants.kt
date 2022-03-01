package liquibase.ext.util

import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import liquibase.Scope
import liquibase.change.Change
import liquibase.change.core.ModifyDataTypeChange
import liquibase.change.core.RenameColumnChange
import liquibase.change.core.RenameTableChange
import liquibase.change.core.RenameViewChange
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.database.core.PostgresDatabase
import liquibase.ext.base.ZdChange
import liquibase.ext.base.ZdStrategy
import liquibase.ext.change.modify.datatype.ZdModifyDatatypeChange
import liquibase.ext.change.rename.column.ZdRenameColumnChange
import liquibase.ext.change.rename.table.ZdRenameTableUsingCopyChange
import liquibase.ext.change.rename.table.ZdRenameTableUsingViewChange
import liquibase.ext.change.rename.view.ZdRenameViewChange

object TestConstants {
    private val dataTypeArb = Arb.of("bigint", "boolean", "int", "float", "decimal", "double", "char", "clob")
    private val alphaNumArb = Arb.stringPattern("[a-zA-Z0-9]+")

    private val renameColumnPairArb =
        Arb.bind(
            alphaNumArb.orNull(),
            alphaNumArb,
            alphaNumArb,
            alphaNumArb
        ) { schema, table, oldColumn, newColumn ->
            ZdRenameColumnChange().apply {
                schemaName = schema
                tableName = table
                oldColumnName = oldColumn
                newColumnName = newColumn
            } to RenameColumnChange().apply {
                schemaName = schema
                tableName = table
                oldColumnName = oldColumn
                newColumnName = newColumn
            }
        }

    private val renameTableUsingCopyPairArb =
        Arb.bind(
            alphaNumArb.orNull(),
            alphaNumArb,
            alphaNumArb,
        ) { schema, oldTable, newTable ->
            ZdRenameTableUsingCopyChange().apply {
                schemaName = schema
                oldTableName = oldTable
                newTableName = newTable
            } to RenameTableChange().apply {
                schemaName = schema
                oldTableName = oldTable
                newTableName = newTable
            }
        }

    private val renameTableUsingViewPairArb =
        Arb.bind(
            alphaNumArb.orNull(),
            alphaNumArb,
            alphaNumArb,
        ) { schema, oldTable, newTable ->
            ZdRenameTableUsingViewChange().apply {
                schemaName = schema
                oldTableName = oldTable
                newTableName = newTable
            } to RenameTableChange().apply {
                schemaName = schema
                oldTableName = oldTable
                newTableName = newTable
            }
        }

    private val modifyDataTypePairArb =
        Arb.bind(
            alphaNumArb.orNull(),
            alphaNumArb,
            alphaNumArb,
            alphaNumArb,
            dataTypeArb
        ) { schema, table, column, newColumn, newType ->
            ZdModifyDatatypeChange().apply {
                schemaName = schema
                tableName = table
                columnName = column
                newColumnName = newColumn
                newDataType = newType
            } to ModifyDataTypeChange().apply {
                schemaName = schema
                tableName = table
                columnName = column
                newDataType = newType
            }
        }

    private val renameViewPairArb =
        Arb.bind(
            alphaNumArb.orNull(),
            alphaNumArb,
            alphaNumArb
        ) { schema, oldView, newView ->
            ZdRenameViewChange().apply {
                schemaName = schema
                oldViewName = oldView
                newViewName = newView
            } to RenameViewChange().apply {
                schemaName = schema
                oldViewName = oldView
                newViewName = newView
            }
        }

    private fun Change.createChangeSet(zdStrategy: ZdStrategy = ZdStrategy.DISABLED): ChangeSet = run {
        val changeLog = mockk<DatabaseChangeLog>(relaxed = true)
        spyk(ChangeSet(changeLog)).apply {
            every { changes } returns listOf(this@createChangeSet)
            val params = ChangeLogParameters()
            params.set(ZdChange.PROPERTY_KEY_ZD_STRATEGY, zdStrategy.toString())
            every { changeLogParameters } returns params
        }
    }

    private fun Pair<Change, Change>.addContext(zdStrategy: ZdStrategy = ZdStrategy.DISABLED) =
        first.apply { changeSet = createChangeSet(zdStrategy) } to
                second.apply { changeSet = createChangeSet(zdStrategy) }

    fun Database.runInScope(scopedRunner: () -> Unit) {
        val scopeObjects: MutableMap<String, Any> = HashMap()
        scopeObjects[Scope.Attr.database.name] = this
        Scope.child(scopeObjects, scopedRunner)
    }

    private val zdChangeArb = Arb.choice(
        renameColumnPairArb,
        renameTableUsingCopyPairArb,
        renameTableUsingViewPairArb,
        modifyDataTypePairArb,
        renameViewPairArb
    )

    val zdDisabledChangeArb = zdChangeArb.map { it.addContext() }
    val zdExpandChangeArb = zdChangeArb.map { it.addContext(ZdStrategy.EXPAND) }
    val zdContractChangeArb = zdChangeArb.map { it.addContext(ZdStrategy.CONTRACT) }

    val supportedDatabases = Arb.of(
        DatabaseFactory.getInstance().implementedDatabases
            .filterIsInstance<PostgresDatabase>()
    )
    val unsupportedDatabases = Arb.of(DatabaseFactory.getInstance().implementedDatabases
        .filterNot { it is PostgresDatabase })
}