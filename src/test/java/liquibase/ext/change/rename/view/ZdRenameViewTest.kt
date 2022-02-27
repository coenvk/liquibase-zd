package liquibase.ext.change.rename.view

import liquibase.ext.change.ZdChangeIntegrationTest

class ZdRenameViewTest
    : ZdChangeIntegrationTest(
    "change/rename/view/original.xml",
    "change/rename/view/expand.xml",
    "change/rename/view/contract.xml",
    changeClass = ZdRenameViewChange::class
)