package liquibase.ext.metadata

import liquibase.CatalogAndSchema
import liquibase.change.AddColumnConfig
import liquibase.change.Change
import liquibase.change.core.*
import liquibase.database.Database
import liquibase.exception.MigrationFailedException
import liquibase.ext.util.KotlinExtensions.ifNotEmptyOrElse
import liquibase.ext.util.KotlinExtensions.throwIfFalse
import liquibase.snapshot.SnapshotControl
import liquibase.snapshot.SnapshotGeneratorFactory
import liquibase.structure.core.*

class MetadataCopy(
    private val catalogName: String?,
    private val schemaName: String?,
    database: Database
) {
    private val snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(
        CatalogAndSchema(catalogName, schemaName), database, SnapshotControl(
            database,
            Table::class.java,
            Column::class.java,
            ForeignKey::class.java,
            PrimaryKey::class.java,
            UniqueConstraint::class.java
        )
    )

    private fun getSnapshotTable(tableName: String): Table =
        snapshot.get(Table::class.java)
            .firstOrNull { it.name == tableName }
            ?: throw MigrationFailedException()

    private fun getSnapshotColumn(table: Table, columnName: String) =
        table.columns
            .firstOrNull { it.name == columnName }
            ?: throw MigrationFailedException()

    fun throwIfPrimaryKey(tableName: String, columnName: String): MetadataCopy {
        getSnapshotTable(tableName).primaryKey.columns
            .none { it.name == columnName }
            .throwIfFalse { MigrationFailedException() }
        return this
    }

    fun copyNullability(tableName: String, oldColumnName: String, newColumnName: String): Change {
        val snapshotColumn = getSnapshotColumn(getSnapshotTable(tableName), oldColumnName)
        if (!snapshotColumn.isNullable) {
            return AddNotNullConstraintChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = tableName
                it.columnName = newColumnName
                it.columnDataType = snapshotColumn.type.typeName
            }
        }
        return EmptyChange()
    }

    fun copyUniqueness(tableName: String, oldColumnName: String, newColumnName: String): Change {
        val snapshotTable = getSnapshotTable(tableName)
        val change = AddUniqueConstraintChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = tableName
        }

        snapshotTable.uniqueConstraints
            .filter { uc -> uc.columns.any { col -> col.name == oldColumnName } }
            .ifNotEmptyOrElse({
                it.forEach { uc ->
                    change.columnNames = uc.columnNames.replace(oldColumnName, newColumnName)
                }
            }, {
                return EmptyChange()
            })

        return change
    }

    fun copyForeignKey(tableName: String, oldColumnName: String, newColumnName: String): Change {
        val snapshotTable = getSnapshotTable(tableName)

        val fkChange = AddForeignKeyConstraintChange().also {
            it.baseTableCatalogName = catalogName
            it.baseTableSchemaName = schemaName
            it.baseTableName = tableName
            it.referencedTableCatalogName = catalogName
            it.referencedTableSchemaName = schemaName
        }

        snapshotTable.outgoingForeignKeys
            .filter { fk -> fk.foreignKeyColumns.any { col -> col.name == oldColumnName } }
            .ifNotEmptyOrElse({
                it.forEach { fk ->
                    fkChange.constraintName =
                        "fk__${tableName}_${newColumnName}__${fk.primaryKeyTable}_${
                            fk.primaryKeyColumns.joinToString(
                                separator = "_"
                            )
                        }"
                    fkChange.baseColumnNames = fk.foreignKeyColumns.joinToString().replace(oldColumnName, newColumnName)
                    fkChange.referencedColumnNames = fk.primaryKeyColumns.joinToString()
                    fkChange.referencedTableName = fk.primaryKeyTable.name
                }
            }, {
                return EmptyChange()
            })

        return fkChange
    }

    fun copyColumnNullable(
        tableName: String,
        oldColumnName: String,
        newColumnName: String
    ): Change {
        val snapshotColumn = getSnapshotColumn(getSnapshotTable(tableName), oldColumnName)

        val newColumn = Column.fromName(newColumnName).setNullable(true)
        val newColumnConfig = AddColumnConfig(newColumn)
        newColumnConfig.type = snapshotColumn.type.typeName

        return AddColumnChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = tableName
            it.columns = listOf(newColumnConfig)
        }
    }
}