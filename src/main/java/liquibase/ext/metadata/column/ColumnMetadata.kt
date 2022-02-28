package liquibase.ext.metadata.column

import liquibase.ext.metadata.Metadata

data class ColumnMetadata(
    var type: String = "",
    var defaultValue: String? = null,
    var isNullable: Boolean = true
) : Metadata {
    val constraints: MutableList<ConstraintMetadata> = mutableListOf()

    fun primaryKeyOrNull(): String? {
        val pkConstraint = constraints.filterIsInstance<PrimaryKeyConstraintMetadata>().firstOrNull() ?: return null
        return pkConstraint.columnNames
    }
}
