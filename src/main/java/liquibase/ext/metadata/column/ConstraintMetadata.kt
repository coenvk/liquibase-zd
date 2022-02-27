package liquibase.ext.metadata.column

import liquibase.change.Change
import liquibase.change.core.*
import liquibase.ext.change.constraint.AddForeignKeyNotValidChange

abstract class ConstraintMetadata(
    var name: String = ""
) {
    abstract fun toChange(
        catalogName: String?,
        schemaName: String?,
        tableName: String,
        oldColumnName: String,
        newColumnName: String
    ): Change

    abstract fun toRollback(
        catalogName: String?,
        schemaName: String?,
        tableName: String
    ): Change
}

class ForeignKeyConstraintMetadata(
    name: String,
    var baseTableName: String = "",
    var baseColumnNames: String = "",
    var referencedTableName: String = "",
    var referencedColumnNames: String = "",
    var isDeferrable: Boolean = false,
    var isDeferred: Boolean = false,
    var validate: Boolean = true,
    var onDelete: Action = Action.NO_ACTION,
    var onUpdate: Action = Action.NO_ACTION
) : ConstraintMetadata(name) {

    override fun toChange(
        catalogName: String?,
        schemaName: String?,
        tableName: String,
        oldColumnName: String,
        newColumnName: String
    ): Change = AddForeignKeyNotValidChange().also {
        it.baseTableCatalogName = catalogName
        it.baseTableSchemaName = schemaName
        it.baseTableName = tableName
        it.baseColumnNames = baseColumnNames.replace(oldColumnName, newColumnName)
        it.referencedTableCatalogName = catalogName
        it.referencedTableSchemaName = schemaName
        it.referencedTableName = referencedTableName
        it.referencedColumnNames = referencedColumnNames
        it.constraintName = "${tableName}_${it.baseColumnNames.replace(',', '_')}_fkey"
        it.deferrable = isDeferrable
        it.initiallyDeferred = isDeferred
        it.validate = false
        it.onDelete = onDelete.toString().replace('_', ' ')
        it.onUpdate = onUpdate.toString().replace('_', ' ')
    }

    override fun toRollback(
        catalogName: String?,
        schemaName: String?,
        tableName: String
    ): Change = DropForeignKeyConstraintChange().also {
        it.baseTableCatalogName = catalogName
        it.baseTableSchemaName = schemaName
        it.baseTableName = tableName
        it.constraintName = name
    }

    enum class Action {
        CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION;

        companion object {
            fun from(char: String): Action {
                return when (char) {
                    "a" -> NO_ACTION
                    "r" -> RESTRICT
                    "c" -> CASCADE
                    "n" -> SET_NULL
                    "d" -> SET_DEFAULT
                    else -> NO_ACTION
                }
            }
        }
    }
}

class UniqueConstraintMetadata(
    name: String = "",
    var columnNames: String = "",
    var isDeferrable: Boolean = false,
    var isDeferred: Boolean = false,
    var validate: Boolean = true
) : ConstraintMetadata(name) {
    override fun toChange(
        catalogName: String?,
        schemaName: String?,
        tableName: String,
        oldColumnName: String,
        newColumnName: String
    ): Change = AddUniqueConstraintChange().also {
        it.catalogName = catalogName
        it.schemaName = schemaName
        it.tableName = tableName
        it.columnNames = columnNames.replace(oldColumnName, newColumnName)
        it.deferrable = isDeferrable
        it.initiallyDeferred = isDeferred
        it.validate = false
    }

    override fun toRollback(
        catalogName: String?,
        schemaName: String?,
        tableName: String
    ): Change = DropUniqueConstraintChange().also {
        it.catalogName = catalogName
        it.schemaName = schemaName
        it.tableName = tableName
        it.constraintName = name
    }
}

class PrimaryKeyConstraintMetadata(
    name: String = "",
    var columnNames: String = "",
    var validate: Boolean = true
) : ConstraintMetadata(name) {
    override fun toChange(
        catalogName: String?,
        schemaName: String?,
        tableName: String,
        oldColumnName: String,
        newColumnName: String
    ): Change = AddPrimaryKeyChange().also {
        it.catalogName = catalogName
        it.schemaName = schemaName
        it.tableName = tableName
        it.columnNames = columnNames.replace(oldColumnName, newColumnName)
        it.validate = false
    }

    override fun toRollback(
        catalogName: String?,
        schemaName: String?,
        tableName: String
    ): Change = DropPrimaryKeyChange().also {
        it.catalogName = catalogName
        it.schemaName = schemaName
        it.tableName = tableName
        it.constraintName = name
    }
}