package liquibase.ext.change.rename.table

import liquibase.ext.change.ZdChangeIntegrationTest

class ZdRenameTableUsingCopyTest
    : ZdChangeIntegrationTest(
    "change/rename/table/copy/original.xml",
    "change/rename/table/copy/expand.xml",
    "change/rename/table/copy/contract.xml",
    changeClass = ZdRenameTableUsingCopyChange::class
)