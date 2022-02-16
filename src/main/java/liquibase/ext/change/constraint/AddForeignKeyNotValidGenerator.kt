package liquibase.ext.change.constraint

import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.exception.ValidationErrors
import liquibase.sql.Sql
import liquibase.sql.UnparsedSql
import liquibase.sqlgenerator.SqlGeneratorChain
import liquibase.sqlgenerator.SqlGeneratorFactory
import liquibase.sqlgenerator.core.AbstractSqlGenerator

class AddForeignKeyNotValidGenerator : AbstractSqlGenerator<AddForeignKeyNotValidStatement>() {
    override fun generateSql(
        statement: AddForeignKeyNotValidStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<AddForeignKeyNotValidStatement>
    ): Array<Sql> = SqlGeneratorFactory.getInstance().generateSql(statement.original, database).mapIndexed { i, e ->
        if (i == 0 && database is PostgresDatabase) {
            UnparsedSql("${e.toSql()} NOT VALID")
        } else e
    }.toTypedArray()

    override fun validate(
        statement: AddForeignKeyNotValidStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<AddForeignKeyNotValidStatement>
    ): ValidationErrors = SqlGeneratorFactory.getInstance().validate(statement.original, database)
}