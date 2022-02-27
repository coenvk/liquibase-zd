package liquibase.ext.base

enum class ZdStrategy(val key: String) {
    DISABLED("disabled"),
    EXPAND("expand"),
    CONTRACT("contract");

    override fun toString(): String {
        return key
    }
}