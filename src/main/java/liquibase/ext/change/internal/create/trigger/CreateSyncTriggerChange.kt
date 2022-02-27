package liquibase.ext.change.internal.create.trigger

import liquibase.change.Change
import liquibase.change.core.CreateProcedureChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.exception.UnexpectedLiquibaseException
import liquibase.ext.change.internal.create.procedure.CreateSyncInsertChange
import liquibase.ext.change.internal.create.procedure.CreateSyncUpdateChange
import liquibase.ext.change.internal.drop.trigger.DropSyncTriggerChange

internal class CreateSyncTriggerChange(
    catalogName: String?,
    schemaName: String?,
    var triggerName: String,
    var tableName: String,
    fromColumnName: String,
    toColumnName: String,
    procedureName: String? = null,
    var onUpdate: Boolean = false,
    var onInsert: Boolean = false
) : CreateProcedureChange() {

    init {
        this.catalogName = catalogName
        this.schemaName = schemaName
        this.procedureName =
            procedureName
                ?: when {
                    onUpdate -> CreateSyncUpdateChange.DEFAULT_PROCEDURE_NAME
                    onInsert -> CreateSyncInsertChange.DEFAULT_PROCEDURE_NAME
                    else -> throw UnexpectedLiquibaseException(
                        "No valid procedure name for the trigger."
                    )
                }
        this.dbms = "postgresql"
        this.procedureText = """CREATE TRIGGER $triggerName
            BEFORE ${if (onUpdate) "UPDATE" else ""}${if (onUpdate && onInsert) " OR INSERT" else if (onInsert) "INSERT" else ""} ON $tableName
            FOR EACH ROW
            WHEN ${if (onUpdate) "(OLD.$fromColumnName IS DISTINCT FROM NEW.$fromColumnName)" else if (onInsert) "(NEW.$fromColumnName IS DISTINCT FROM NEW.$toColumnName)" else ""}
            EXECUTE PROCEDURE ${this.procedureName}(${fromColumnName}, ${toColumnName});
            """
    }

    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    override fun createInverses(): Array<Change> {
        return arrayOf(
            DropSyncTriggerChange(
                triggerName,
                tableName
            )
        )
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
    return arrayOf(
        CreateSyncUpdateChange(
            catalogName,
            schemaName
        ),
        CreateSyncTriggerChange(
            catalogName,
            schemaName,
            triggerName,
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
    val procedureName = "sync_on_insert"
    return arrayOf(
        CreateSyncInsertChange(
            catalogName,
            schemaName,
            procedureName
        ),
        CreateSyncTriggerChange(
            catalogName,
            schemaName,
            triggerName,
            tableName,
            columnName1,
            columnName2,
            onInsert = true
        )
    )
}