package liquibase.ext.change.copy.table

import liquibase.statement.AbstractSqlStatement

class CopyTableStatement(
    val catalogName: String? = null,
    val schemaName: String? = null,
    val tableName: String,
    val copyTableName: String
) : AbstractSqlStatement()