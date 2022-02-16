package liquibase.ext.base

enum class ZdStrategy(val key: String) {
    OFF("off"),
    EXPAND("expand"),
    CONTRACT("contract");

    override fun toString(): String {
        return key
    }
}