package liquibase.ext.executor

import liquibase.executor.jvm.JdbcExecutor
import liquibase.ext.change.custom.CustomTaskStatement
import liquibase.sql.visitor.SqlVisitor
import liquibase.statement.SqlStatement

class CustomJdbcExecutor : JdbcExecutor() {

    override fun getPriority(): Int = super.getPriority() + 1

    override fun execute(sql: SqlStatement, sqlVisitors: MutableList<SqlVisitor>) = when (sql) {
        is CustomTaskStatement -> sql.execute(database)
        else -> super.execute(sql, sqlVisitors)
    }
}