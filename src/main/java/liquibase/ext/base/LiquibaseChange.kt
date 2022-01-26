package liquibase.ext.base

import liquibase.change.Change
import liquibase.database.Database
import liquibase.statement.SqlStatement
import java.lang.reflect.Method

interface LiquibaseChange<in T : Change> : Change {
    fun generateOriginalStatements(database: Database): Array<SqlStatement>
}

inline fun <reified T : Change> T.wrapChange(): LiquibaseChange<T> {
    return object : LiquibaseChange<T>, Change by this {
        override fun generateOriginalStatements(database: Database): Array<SqlStatement> {
            return this@wrapChange.generateStatements(database)
        }
    }
}

internal fun Any.invokeHiddenMethod(methodName: String, vararg args: Any): Any? {
    val method: Method
    try {
        method = this::class.java.getDeclaredMethod(methodName)
    } catch (e: NoSuchMethodException) {
        return null
    } finally {
        true.also { method.isAccessible = it }
    }
    return method.invoke(this, args)
}