package liquibase.ext.change.constraint

import liquibase.database.Database
import liquibase.ext.base.RewriteSqlGenerator
import liquibase.sql.Sql
import liquibase.sql.UnparsedSql
import liquibase.sqlgenerator.SqlGeneratorChain

class AddForeignKeyNotValidGenerator : RewriteSqlGenerator<AddForeignKeyNotValidStatement>() {
    override fun generateSql(
        statement: AddForeignKeyNotValidStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<AddForeignKeyNotValidStatement>
    ): Array<Sql> = generateOriginal(statement, database).rewriteSql(database) { sql ->
        UnparsedSql("${sql.toSql()} NOT VALID")
    }
}