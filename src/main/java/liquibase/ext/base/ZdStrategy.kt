package liquibase.ext.base

enum class ZdStrategy(private val key: String) {
    DISABLED("disabled"),
    EXPAND("expand"),
    CONTRACT("contract");

    override fun toString(): String {
        return key
    }
}