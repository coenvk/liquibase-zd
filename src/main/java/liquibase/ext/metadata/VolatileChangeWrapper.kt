package liquibase.ext.metadata

import liquibase.change.Change
import liquibase.database.Database

internal class VolatileChangeWrapper<in C : Change>(change: C) : Change by change {
    override fun generateStatementsVolatile(database: Database): Boolean = true
}

fun Change.toVolatile(): Change = VolatileChangeWrapper(this)