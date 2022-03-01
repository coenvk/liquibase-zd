package liquibase.ext.change.rename.table

import liquibase.ext.change.ZdChangeIntegrationTest

class ZdRenameTableUsingViewTest
    : ZdChangeIntegrationTest(
    "change/rename/table/view/original.xml",
    "change/rename/table/view/expand.xml",
    "change/rename/table/view/contract.xml",
    changeClass = ZdRenameTableUsingViewChange::class
)