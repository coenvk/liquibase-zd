package liquibase.ext.change.load

import liquibase.database.Database
import liquibase.exception.ValidationErrors
import liquibase.sql.Sql
import liquibase.sql.UnparsedSql
import liquibase.sqlgenerator.SqlGeneratorChain
import liquibase.sqlgenerator.core.AbstractSqlGenerator
import liquibase.structure.core.Relation
import liquibase.structure.core.Table

class LoadLargeDataGenerator : AbstractSqlGenerator<LoadLargeDataStatement>() {
    override fun validate(
        statement: LoadLargeDataStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<LoadLargeDataStatement>
    ): ValidationErrors {
        val validationErrors = ValidationErrors()
        validationErrors.checkRequiredField("tableName", statement.tableName)
        validationErrors.checkRequiredField("file", statement.fileName)
        return validationErrors
    }

    override fun generateSql(
        statement: LoadLargeDataStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<LoadLargeDataStatement>
    ): Array<Sql> {
        val sb = StringBuilder(
            "COPY ${
                database.escapeTableName(
                    statement.catalogName,
                    statement.schemaName,
                    statement.tableName
                )
            }"
        )
        sb.append(
            "(${
                statement.columns.joinToString {
                    database.escapeColumnName(
                        statement.catalogName,
                        statement.schemaName,
                        statement.tableName,
                        it.name
                    )
                }
            }) "
        )
        if (statement.fileName.endsWith(".gz")) {
            sb.append("FROM PROGRAM 'gzip -dc ")
        } else sb.append("FROM '")
        sb.append(
            "${statement.fileName}' (FORMAT ${statement.format}, FREEZE ${statement.freeze}, " +
                    "DELIMITER E'${statement.delimiter}', NULL '${statement.nullString}', HEADER ${statement.header}, " +
                    "QUOTE E'${statement.quote}', ENCODING '${statement.encoding}'"
        )
        if (statement.escape != null) {
            sb.append(", ESCAPE '${statement.escape}'")
        }
        sb.append(")")
        return arrayOf(UnparsedSql(sb.toString(), getAffectedTable(statement)))
    }

    private fun getAffectedTable(statement: LoadLargeDataStatement): Relation? {
        return Table().setName(statement.tableName).setSchema(statement.catalogName, statement.schemaName)
    }
}