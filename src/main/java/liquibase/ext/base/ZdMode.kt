package liquibase.ext.base

enum class ZdMode(name: String, private val description: String) {
    OFF("off", "Do not transform any change operations for zero downtime."),
    EXPAND("expand", "Transform the change operation into the expand step."),
    CONTRACT("contract", "Transform the change operation into the contract step.");

    override fun toString(): String {
        return name.lowercase()
    }
}