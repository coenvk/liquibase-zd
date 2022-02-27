package liquibase.ext.util

import java.sql.ResultSet

internal object KotlinExtensions {
    inline fun <reified T, R : T> Array<T>.mapIf(condition: Boolean, f: (element: T) -> R): Array<T> = if (condition) {
        map(f).toTypedArray()
    } else this

    inline fun <reified T, R : T> Array<T>.mapIndexedIf(condition: Boolean, f: (idx: Int, element: T) -> R): Array<T> =
        if (condition) {
            mapIndexed(f).toTypedArray()
        } else this

    inline fun <reified T> Array<T>.filterIf(condition: Boolean, f: (element: T) -> Boolean): Array<T> =
        if (condition) {
            filter(f).toTypedArray()
        } else this

    inline fun <reified T, R : T> Array<T>.mapFirst(f: (element: T) -> R): Array<T> = mapIndexed { i, e ->
        if (i == 0) f(e)
        else e
    }.toTypedArray()

    inline fun <reified T> Array<T>.throwIf(throwable: Throwable, condition: (Array<T>) -> Boolean): Array<T> {
        if (condition(this)) {
            throw throwable
        } else return this
    }

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

    inline fun ResultSet.forEach(action: (ResultSet) -> Unit): Unit = with(this) {
        while (next()) {
            action(this)
        }
    }

    inline fun ResultSet.forFirst(action: (ResultSet) -> Unit): Unit = with(this) {
        if (next()) {
            action(this)
        }
    }
}