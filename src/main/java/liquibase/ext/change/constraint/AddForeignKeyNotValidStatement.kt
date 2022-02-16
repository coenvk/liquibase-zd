package liquibase.ext.change.constraint

import liquibase.statement.AbstractSqlStatement
import liquibase.statement.SqlStatement

data class AddForeignKeyNotValidStatement(val original: SqlStatement) : AbstractSqlStatement()