package liquibase.ext.metadata

import liquibase.database.Database

internal interface MetadataCopyTask<T> {
    fun setUp()
    fun copy(database: Database, targetObject: T, args: Map<String, Any>): T
}