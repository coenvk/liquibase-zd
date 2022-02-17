package liquibase.ext.change.constraint

import liquibase.statement.AbstractSqlStatement
import liquibase.statement.SqlStatement

interface SqlStatementWrapper : SqlStatement {
    val original: SqlStatement
}

data class AddForeignKeyNotValidStatement(override val original: SqlStatement) : AbstractSqlStatement(),
    SqlStatementWrapper

data class AddCheckConstraintNotValidStatement(override val original: SqlStatement) : AbstractSqlStatement(),
    SqlStatementWrapper