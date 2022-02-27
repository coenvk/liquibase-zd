package liquibase.ext.change.rename.table

import liquibase.ext.change.ZdChangeIntegrationTest

class ZdRenameTableUsingViewTest
    : ZdChangeIntegrationTest(
    "change/rename/view/original.xml",
    "change/rename/view/expand.xml",
    "change/rename/view/contract.xml",
    changeClass = ZdRenameTableUsingViewChange::class
)