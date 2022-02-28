package liquibase.ext.change.internal.create.procedure

import liquibase.change.core.CreateProcedureChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase

internal class CreateSyncInsertChange(
    catalogName: String?,
    schemaName: String?,
    columnName1: String,
    columnName2: String,
    procedureName: String = DEFAULT_PROCEDURE_NAME
) : CreateProcedureChange() {
    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    init {
        this.catalogName = catalogName
        this.schemaName = schemaName
        this.procedureName = procedureName
        this.dbms = "postgresql"
        this.procedureText = """
            CREATE OR REPLACE FUNCTION $procedureName() RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS ${"$"}$
                    BEGIN
                        IF NEW.${columnName2} IS NOT NULL THEN
                            NEW.${columnName1} := NEW.${columnName2};
                        ELSE
                            NEW.${columnName2} := NEW.${columnName1};
                        END IF;
                        RETURN NEW;
                    END;
                    ${"$"}$;
        """.trimIndent()
    }

    companion object {
        internal const val DEFAULT_PROCEDURE_NAME = "sync_on_insert"
    }
}