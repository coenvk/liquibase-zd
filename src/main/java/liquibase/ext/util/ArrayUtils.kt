package liquibase.ext.util

object ArrayUtils {
    inline fun <reified A, reified T> Array<T>?.ifNullOrEmpty(
        args: Array<out A>,
        generateOthers: (Array<out A>) -> Array<T>
    ): Array<T> = if (isNullOrEmpty()) {
        generateOthers(args)
    } else this

    inline fun <reified A, reified T> Array<T>?.ifNullOrEmpty(
        arg: A,
        generateOthers: (A) -> Array<T>
    ): Array<T> = if (isNullOrEmpty()) {
        generateOthers(arg)
    } else this

    inline fun <reified T> Array<T>?.ifNullOrEmpty(
        generateOthers: () -> Array<T>
    ): Array<T> = if (isNullOrEmpty()) {
        generateOthers()
    } else this

    inline fun <reified T> Array<T>?.ifEmpty(
        generateOthers: () -> Array<T>
    ): Array<T>? = if (this != null && isEmpty()) {
        generateOthers()
    } else this
}