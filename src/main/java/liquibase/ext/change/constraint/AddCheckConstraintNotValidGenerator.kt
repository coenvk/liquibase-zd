package liquibase.ext.change.constraint

import liquibase.database.Database
import liquibase.ext.base.RewriteSqlGenerator
import liquibase.sql.Sql
import liquibase.sql.UnparsedSql
import liquibase.sqlgenerator.SqlGeneratorChain

class AddCheckConstraintNotValidGenerator : RewriteSqlGenerator<AddCheckConstraintNotValidStatement>() {
    override fun generateSql(
        statement: AddCheckConstraintNotValidStatement,
        database: Database,
        sqlGeneratorChain: SqlGeneratorChain<AddCheckConstraintNotValidStatement>
    ): Array<Sql> = generateOriginal(statement, database).rewriteSql(database) { sql ->
        UnparsedSql("${sql.toSql()} NOT VALID")
    }
}