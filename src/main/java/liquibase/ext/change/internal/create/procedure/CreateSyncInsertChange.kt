package liquibase.ext.change.internal.create.procedure

import liquibase.change.core.CreateProcedureChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase

internal class CreateSyncInsertChange(
    catalogName: String?,
    schemaName: String?,
    procedureName: String = DEFAULT_PROCEDURE_NAME
) : CreateProcedureChange() {
    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    init {
        this.catalogName = catalogName
        this.schemaName = schemaName
        this.procedureName = procedureName
        this.dbms = "postgresql"
        this.procedureText = """
            CREATE OR REPLACE FUNCTION $procedureName(column1 text, column2 text) RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS ${"$"}${"$"}
                    BEGIN
                        IF NEW.column1 IS NOT NULL THEN
                            NEW.column1 := NEW.column2;
                        ELSE
                            NEW.column2 := NEW.column1;
                        END IF;
                        RETURN NEW;
                    END ${"$"}${"$"};
        """.trimIndent()
    }

    companion object {
        internal val DEFAULT_PROCEDURE_NAME = "sync_on_insert"
    }
}