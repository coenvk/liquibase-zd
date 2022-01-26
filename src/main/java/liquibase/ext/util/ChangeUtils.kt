package liquibase.ext.util

import liquibase.change.Change
import liquibase.ext.base.ZdMode
import java.util.*

object ChangeUtils {
    private const val PROPERTY_KEY_ZD_MODE = "zd-mode"
    private val DEFAULT_MODE = ZdMode.OFF

    fun getMode(change: Change): ZdMode {
        val changeSet = change.changeSet ?: return DEFAULT_MODE
        val modeName = changeSet.changeLogParameters.getValue(
            PROPERTY_KEY_ZD_MODE,
            changeSet.changeLog
        ) as String
        return Arrays
            .stream(ZdMode.values())
            .filter { m: ZdMode -> m.name.lowercase() == modeName.lowercase() }
            .findAny()
            .orElse(DEFAULT_MODE)
    }
}