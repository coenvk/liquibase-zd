package liquibase.ext.change.modify.datatype

import liquibase.change.Change
import liquibase.change.core.ModifyDataTypeChange
import liquibase.database.Database
import liquibase.ext.base.AbstractZdChange

class ZdModifyDataTypeChange(
    originalChange: ModifyDataTypeChange
) : AbstractZdChange<ModifyDataTypeChange>(originalChange) {
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