package liquibase.ext.change.create.procedure

import liquibase.change.core.CreateProcedureChange
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase

class CreateSyncUpdateChange(
    catalogName: String?,
    schemaName: String?,
    procedureName: String,
    fromColumnName: String,
    toColumnName: String,
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
                        IF NEW.$fromColumnName IS DISTINCT FROM OLD.$fromColumnName THEN
                            NEW.$toColumnName := NEW.$fromColumnName;
                        END IF;
                        RETURN NEW;
                    END;
                    ${"$"}$;
                    """
    }

//    override fun createInverses(): Array<Change> {
//        return arrayOf(DropProcedureChange().also {
//            it.catalogName = catalogName
//            it.schemaName = schemaName
//            it.procedureName = procedureName
//        })
//    }
}