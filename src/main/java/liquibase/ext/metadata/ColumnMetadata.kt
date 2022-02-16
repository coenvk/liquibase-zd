package liquibase.ext.metadata

data class ColumnMetadata(
    var name: String? = null,
    var type: String? = null,
    var defaultValue: String? = null,
    var isNullable: Boolean = true
) {
    val constraints: MutableList<ConstraintMetadata> = mutableListOf()
}
