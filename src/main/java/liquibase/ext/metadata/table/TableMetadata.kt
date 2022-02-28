package liquibase.ext.metadata.table

import liquibase.ext.metadata.Metadata

data class TableMetadata(
    val columnNames: MutableCollection<String> = mutableListOf(),
    val primaryColumnNames: MutableCollection<String> = mutableListOf()
) : Metadata