package liquibase.ext.change.modify.datatype

import liquibase.change.Change
import liquibase.change.core.ModifyDataTypeChange
import liquibase.database.Database
import liquibase.ext.base.AbstractZdChange
import liquibase.statement.SqlStatement

class ZdModifyDataTypeChange(
    override val originalChange: ModifyDataTypeChange,
    override val generateOriginalStatements: (Database) -> Array<SqlStatement>
) : AbstractZdChange<ModifyDataTypeChange>() {
    override fun generateExpandChanges(database: Database): Array<Change> {
        TODO("Not yet implemented")
    }

    override fun generateContractChanges(database: Database): Array<Change> {
        TODO("Not yet implemented")
    }

    override fun createExpandInverses(): Array<Change> {
        TODO("Not yet implemented")
    }
}