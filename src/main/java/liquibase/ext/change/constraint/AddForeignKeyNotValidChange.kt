package liquibase.ext.change.constraint

import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.change.core.AddForeignKeyConstraintChange
import liquibase.database.Database
import liquibase.ext.base.RewritableChange
import liquibase.statement.SqlStatement
import liquibase.statement.core.AddForeignKeyConstraintStatement

@DatabaseChange(
    name = "addForeignKeyConstraint",
    description = "Adds a foreign key constraint to an existing column.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["column"]
)
class AddForeignKeyNotValidChange : AddForeignKeyConstraintChange(), RewritableChange {
    override fun generateStatements(database: Database): Array<SqlStatement> =
        super.generateStatements(database).rewriteStatements(database) { stmt ->
            when (stmt) {
                is AddForeignKeyConstraintStatement -> AddForeignKeyNotValidStatement(stmt)
                else -> stmt
            }
        }
}