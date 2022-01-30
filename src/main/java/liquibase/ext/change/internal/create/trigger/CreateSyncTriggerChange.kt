package liquibase.ext.change.internal.create.trigger

import liquibase.change.Change
import liquibase.change.core.CreateProcedureChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.ext.change.internal.create.procedure.CreateSyncInsertChange
import liquibase.ext.change.internal.create.procedure.CreateSyncUpdateChange
import liquibase.ext.change.internal.drop.trigger.DropSyncTriggerChange

internal class CreateSyncTriggerChange(
    catalogName: String?,
    schemaName: String?,
    var triggerName: String,
    procedureName: String,
    var tableName: String,
    fromColumnName: String,
    toColumnName: String,
    var onUpdate: Boolean = false,
    var onInsert: Boolean = false
) : CreateProcedureChange() {

    init {
        this.catalogName = catalogName
        this.schemaName = schemaName
        this.procedureName = procedureName
        this.dbms = "postgresql"
        this.procedureText = """CREATE TRIGGER $triggerName
            BEFORE ${if (onUpdate) "UPDATE" else ""} ${if (onUpdate && onInsert) "OR INSERT" else if (onInsert) "INSERT" else ""} ON $tableName
            FOR EACH ROW
            ${if (onUpdate) "WHEN (OLD.$fromColumnName IS DISTINCT FROM NEW.$fromColumnName)" else if (onInsert) "WHEN (NEW.$fromColumnName IS DISTINCT FROM NEW.$toColumnName)" else ""}
            EXECUTE PROCEDURE $procedureName();
            """
    }

    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    override fun createInverses(): Array<Change> {
        return arrayOf(DropSyncTriggerChange(
            triggerName,
            tableName
        ))
    }
}

fun syncUpdateTriggerChange(
    catalogName: String?,
    schemaName: String?,
    tableName: String,
    triggerName: String,
    fromColumnName: String,
    toColumnName: String,
): Array<Change> {
    val procedureName = "${triggerName}_${tableName}_on_update"
    return arrayOf(
        CreateSyncUpdateChange(
            catalogName,
            schemaName,
            procedureName,
            fromColumnName,
            toColumnName
        ),
        CreateSyncTriggerChange(
            catalogName,
            schemaName,
            triggerName,
            procedureName,
            tableName,
            fromColumnName,
            toColumnName,
            onUpdate = true
        )
    )
}

fun syncInsertTriggerChange(
    catalogName: String?,
    schemaName: String?,
    tableName: String,
    triggerName: String,
    columnName1: String,
    columnName2: String,
): Array<Change> {
    val procedureName = "${triggerName}_${tableName}_on_insert"
    return arrayOf(
        CreateSyncInsertChange(
            catalogName,
            schemaName,
            procedureName,
            columnName1,
            columnName2
        ),
        CreateSyncTriggerChange(
            catalogName,
            schemaName,
            triggerName,
            procedureName,
            tableName,
            columnName1,
            columnName2,
            onInsert = true
        )
    )
}