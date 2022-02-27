package liquibase.ext.metadata.view

import liquibase.ext.metadata.Metadata

data class ViewMetadata(
    var definition: String = ""
) : Metadata