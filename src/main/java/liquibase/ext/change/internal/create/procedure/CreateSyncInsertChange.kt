package liquibase.ext.change.internal.create.procedure

import liquibase.change.core.CreateProcedureChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase

internal class CreateSyncInsertChange(
    catalogName: String?,
    schemaName: String?,
    procedureName: String,
    columnName1: String,
    columnName2: String
) : CreateProcedureChange() {
    override fun supports(database: Database): Boolean = super.supports(database) && database is PostgresDatabase

    init {
        this.catalogName = catalogName
        this.schemaName = schemaName
        this.procedureName = procedureName
        this.dbms = "postgresql"
        this.procedureText = """CREATE OR REPLACE FUNCTION $procedureName() RETURNS TRIGGER
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
                    """
    }
}