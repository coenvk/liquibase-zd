package liquibase.ext.change.rename.table

import liquibase.change.Change
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.CreateProcedureChange
import liquibase.change.core.DropTableChange
import liquibase.change.core.RenameTableChange
import liquibase.database.Database
import liquibase.ext.base.ZdChange
import liquibase.ext.change.copy.table.CopyTableChange
import liquibase.ext.change.custom.CustomChangeDecorator
import liquibase.ext.change.internal.drop.trigger.DropSyncTriggerChange
import liquibase.ext.change.update.BulkColumnCopyChange
import liquibase.ext.change.update.BulkTableCopyChange
import liquibase.ext.metadata.table.TableCopyTask
import liquibase.ext.metadata.table.TableMetadata
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "renameTableUsingCopy",
    description = "Renames an existing table.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["table"]
)
class ZdRenameTableUsingCopyChange : RenameTableChange(), ZdChange {
    var batchChunkSize: Long? = BulkColumnCopyChange.DEFAULT_CHUNK_SIZE
    var batchSleepTime: Long? = BulkColumnCopyChange.DEFAULT_SLEEP_TIME

    override fun generateStatements(database: Database): Array<SqlStatement> =
        generateZdStatements(database) { super.generateStatements(it) }

    override fun createInverses(): Array<Change>? =
        createZdInverses { super.createInverses() }

    override fun generateStatementsVolatile(database: Database): Boolean = isExpand(database)
    override fun generateRollbackStatementsVolatile(database: Database): Boolean = false

    override fun generateExpandChanges(database: Database): Array<Change> {
        val columnNamesMetadata = TableCopyTask().copy(
            database, TableMetadata(), mapOf(
                "tableName" to oldTableName
            )
        )
        val columnNames = columnNamesMetadata.columnNames.joinToString()
        val newColumnNames = columnNamesMetadata.columnNames.joinToString { "NEW.$it" }
        val rowId = columnNamesMetadata.primaryColumnNames.joinToString()
        val whereRowIdString =
            columnNamesMetadata.primaryColumnNames.joinToString(separator = " AND ") { "$it = OLD.$it" }
        return arrayOf(
            CopyTableChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.tableName = oldTableName
                it.copyTableName = newTableName
            },
            CreateProcedureChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.procedureName = "t1_sync_$oldTableName"
                it.dbms = "postgresql"
                it.procedureText = """
                CREATE OR REPLACE FUNCTION ${it.procedureName}() RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS ${"$"}$
                    BEGIN
                        IF (TG_OP = 'UPDATE') THEN
                            INSERT INTO $newTableName
                            SELECT NEW.*
                            ON CONFLICT ($rowId) DO UPDATE
                                SET ($columnNames) = ($newColumnNames);
                        ELSIF (TG_OP = 'INSERT') THEN
                            INSERT INTO $newTableName
                            SELECT NEW.*;
                        ELSIF (TG_OP = 'DELETE') THEN
                            DELETE FROM $newTableName
                            WHERE $whereRowIdString;
                        END IF;
                    END;
                    ${"$"}$;
            """.trimIndent()
            },
            CreateProcedureChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.procedureName = "t1_sync_$oldTableName"
                it.dbms = "postgresql"
                it.procedureText = """
                CREATE OR REPLACE TRIGGER t1
                BEFORE UPDATE OR INSERT OR DELETE ON $oldTableName
                FOR EACH ROW
                EXECUTE PROCEDURE ${it.procedureName}();
            """.trimIndent()
            },
            CustomChangeDecorator().setClass(BulkTableCopyChange::class.java.name).also {
                it.setParam("fromCatalogName", catalogName)
                it.setParam("toCatalogName", catalogName)
                it.setParam("fromSchemaName", schemaName)
                it.setParam("toSchemaName", schemaName)
                it.setParam("fromTableName", oldTableName)
                it.setParam("toTableName", newTableName)
                it.setParam("rowId", rowId)
                it.setParam("chunkSize", batchChunkSize?.toString())
                it.setParam("sleepTime", batchSleepTime?.toString())
            },
            CreateProcedureChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.procedureName = "t2_sync_$newTableName"
                it.dbms = "postgresql"
                it.procedureText = """
                CREATE OR REPLACE FUNCTION ${it.procedureName}() RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS ${"$"}$
                    BEGIN
                        IF (TG_OP = 'UPDATE') THEN
                            UPDATE $oldTableName
                            SET ($columnNames) = ($newColumnNames)
                            WHERE $whereRowIdString;
                        ELSIF (TG_OP = 'INSERT') THEN
                            INSERT INTO $oldTableName
                            SELECT NEW.*;
                        ELSIF (TG_OP = 'DELETE') THEN
                            DELETE FROM $oldTableName
                            WHERE $whereRowIdString;
                        END IF;
                    END;
                    ${"$"}$;
            """.trimIndent()
            },
            CreateProcedureChange().also {
                it.catalogName = catalogName
                it.schemaName = schemaName
                it.procedureName = "t2_sync_$newTableName"
                it.dbms = "postgresql"
                it.procedureText = """
                CREATE OR REPLACE TRIGGER t2
                BEFORE UPDATE OR INSERT OR DELETE ON $newTableName
                FOR EACH ROW
                EXECUTE PROCEDURE ${it.procedureName}();
            """.trimIndent()
            }
        )
    }

    override fun generateContractChanges(database: Database): Array<Change> = arrayOf(
        DropSyncTriggerChange(
            "t2",
            newTableName
        ),
        DropTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = oldTableName
            it.isCascadeConstraints = true
        }
    )

    override fun createExpandInverses(): Array<Change> = arrayOf(
        DropSyncTriggerChange(
            "t1",
            oldTableName
        ),
        DropTableChange().also {
            it.catalogName = catalogName
            it.schemaName = schemaName
            it.tableName = newTableName
            it.isCascadeConstraints = true
        }
    )
}