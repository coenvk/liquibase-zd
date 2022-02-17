package liquibase.ext.change.constraint

import com.datical.liquibase.ext.storedlogic.checkconstraint.change.AddCheckConstraintChange
import com.datical.liquibase.ext.storedlogic.checkconstraint.change.AddCheckConstraintStatement
import liquibase.change.ChangeMetaData
import liquibase.change.DatabaseChange
import liquibase.database.Database
import liquibase.ext.base.RewritableChange
import liquibase.statement.SqlStatement

@DatabaseChange(
    name = "addCheckConstraint",
    description = "Adds a check constraint to an existing column or set of columns.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 1,
    appliesTo = ["column"]
)
class AddCheckConstraintNotValidChange : AddCheckConstraintChange(), RewritableChange {
    override fun generateStatements(database: Database): Array<SqlStatement> =
        super.generateStatements(database).rewriteStatements(database) { stmt ->
            when (stmt) {
                is AddCheckConstraintStatement -> AddCheckConstraintNotValidStatement(stmt)
                else -> stmt
            }
        }
}