package liquibase.ext.change.rename.column

import liquibase.ext.change.ZdChangeIntegrationTest

class ZdRenameColumnTest
    : ZdChangeIntegrationTest(
    "change/rename/column/original.xml",
    "change/rename/column/expand.xml",
    "change/rename/column/contract.xml",
    changeClass = ZdRenameColumnChange::class
)