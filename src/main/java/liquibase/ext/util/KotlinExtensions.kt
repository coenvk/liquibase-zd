package liquibase.ext.util

import java.sql.ResultSet

object KotlinExtensions {
    inline fun <reified T, R : T> Array<T>.mapIf(condition: Boolean, f: (element: T) -> R): Array<T> = if (condition) {
        map(f).toTypedArray()
    } else this

    inline fun Boolean.throwIfFalse(
        throwable: () -> Throwable
    ) {
        if (!this) throw throwable()
    }

    inline fun <reified T> Collection<T>.ifNotEmptyOrElse(
        ifNotEmpty: (Collection<T>) -> Unit,
        orElse: (Collection<T>) -> Unit
    ) {
        if (isNotEmpty()) {
            ifNotEmpty(this)
        } else {
            orElse(this)
        }
    }

    inline fun <reified T> ResultSet.getAll(columnIndex: Int): Collection<T> = with(this) {
        val res = mutableListOf<T>()
        while (next()) {
            res.add(getObject(columnIndex) as T)
        }
        return res
    }
}