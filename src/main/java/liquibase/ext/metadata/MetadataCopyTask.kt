package liquibase.ext.metadata

import liquibase.database.Database

interface MetadataCopyTask<T : Metadata> {
    fun setUp()
    fun copy(database: Database, targetObject: T, args: Map<String, Any>): T
}