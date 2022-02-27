package liquibase.ext.change.copy.table

import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.exception.ValidationErrors
import liquibase.sql.Sql
import liquibase.sql.UnparsedSql
import liquibase.sqlgenerator.SqlGeneratorChain
import liquibase.sqlgenerator.core.AbstractSqlGenerator

class CopyTableGenerator : AbstractSqlGenerator<CopyTableStatement>() {
    override fun supports(statement: CopyTableStatement, database: Database): Boolean = database is PostgresDatabase

    override fun validate(
        statement: CopyTableStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<CopyTableStatement>
    ): ValidationErrors {
        val validationErrors = ValidationErrors()
        validationErrors.checkRequiredField("tableName", statement.tableName)
        validationErrors.checkRequiredField("copyTableName", statement.copyTableName)
        return validationErrors
    }

    override fun generateSql(
        statement: CopyTableStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<CopyTableStatement>
    ): Array<Sql> {
        val sql = """
            create or replace function create_table_like(source_table text, new_table text)
            returns void language plpgsql
            as ${'$'}${'$'}
            declare
                rec record;
            begin
                execute format(
                    'create table %s (like %s including all)',
                    new_table, source_table);
                for rec in
                    select oid, conname
                    from pg_constraint
                    where contype = 'f' 
                    and conrelid = source_table::regclass
                loop
                    execute format(
                        'alter table %s add constraint %s %s',
                        new_table,
                        replace(rec.conname, source_table, new_table),
                        pg_get_constraintdef(rec.oid));
                end loop;
            end ${'$'}${'$'};
            select create_table_like('${statement.tableName}', '${statement.copyTableName}');
        """.trimIndent()
        return arrayOf(UnparsedSql(sql))
    }
}