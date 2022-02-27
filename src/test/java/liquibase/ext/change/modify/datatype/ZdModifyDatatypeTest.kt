package liquibase.ext.change.modify.datatype

import liquibase.ext.change.ZdChangeIntegrationTest

class ZdModifyDatatypeTest
    : ZdChangeIntegrationTest(
    "change/modify/datatype/original.xml",
    "change/modify/datatype/expand.xml",
    "change/modify/datatype/contract.xml",
    changeClass = ZdModifyDatatypeChange::class
)