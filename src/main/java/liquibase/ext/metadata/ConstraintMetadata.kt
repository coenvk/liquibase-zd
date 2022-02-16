package liquibase.ext.metadata

import liquibase.change.Change
import liquibase.change.core.AddForeignKeyConstraintChange
import liquibase.change.core.AddPrimaryKeyChange
import liquibase.change.core.AddUniqueConstraintChange
import java.util.*

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
    ): Change = AddForeignKeyConstraintChange().also {
        it.baseTableCatalogName = catalogName
        it.baseTableSchemaName = schemaName
        it.baseTableName = tableName
        it.baseColumnNames = baseColumnNames.replace(oldColumnName, newColumnName)
        it.referencedTableCatalogName = catalogName
        it.referencedTableSchemaName = schemaName
        it.referencedTableName = referencedTableName
        it.referencedColumnNames = referencedColumnNames
        it.constraintName = "fk_${UUID.randomUUID()}"
        it.deferrable = isDeferrable
        it.initiallyDeferred = isDeferred
        it.validate = false
        it.onDelete = onDelete.toString().replace('_', ' ')
        it.onUpdate = onUpdate.toString().replace('_', ' ')
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
}