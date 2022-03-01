package liquibase.ext.util

import org.junit.jupiter.api.assertThrows

internal object TestKotlinExtensions {
    inline fun <reified T : Throwable> assertThrowsIf(
        condition: Boolean,
        executable: () -> Unit
    ): Boolean {
        if (condition) {
            assertThrows<T> { executable() }
            return true
        } else executable()
        return false
    }
}