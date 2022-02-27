package liquibase.ext.change

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.checkAll
import liquibase.change.Change
import liquibase.database.Database
import liquibase.exception.RollbackImpossibleException
import liquibase.ext.util.TestConstants
import liquibase.ext.util.TestConstants.runInScope
import liquibase.sqlgenerator.SqlGeneratorFactory
import kotlin.test.assertFailsWith

class ZdChangeTest : ShouldSpec({
    val generator = SqlGeneratorFactory.getInstance()

    fun Change.toSql(db: Database): List<String> = try {
        generator.generateSql(this, db).map { it.toSql() }
    } catch (e: IllegalStateException) {
        emptyList()
    }

    fun Change.toRollbackSql(db: Database): List<String> = try {
        generator.generateSql(this.generateRollbackStatements(db), db).map { it.toSql() }
    } catch (e: IllegalStateException) {
        emptyList()
    }

    context("Validation") {
        should("rewrite expand statements when supported database is used") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                val expand = zdChange.toSql(db)
                val original = originalChange.toSql(db)

                assert(expand.none { it in original })
            }
        }
        should("rewrite contract statements when supported database is used") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdContractChangeArb
            ) { db, (zdChange, originalChange) ->
                val contract = zdChange.toSql(db)
                val original = originalChange.toSql(db)

                assert(contract.none { it in original })
            }
        }
        should("not rewrite statements when zero-downtime is off") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdOffChangeArb
            ) { db, (zdChange, originalChange) ->
                val off = zdChange.toSql(db)
                val original = originalChange.toSql(db)

                assert(off.all { it in original })
            }
        }
        should("not rewrite expand statements when unsupported database is used") {
            checkAll(
                TestConstants.unsupportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                val expand = zdChange.toSql(db)
                val original = originalChange.toSql(db)

                assert(expand.all { it in original })
            }
        }
        should("not rewrite contract statements when unsupported database is used") {
            checkAll(
                TestConstants.unsupportedDatabases,
                TestConstants.zdContractChangeArb
            ) { db, (zdChange, originalChange) ->
                val contract = zdChange.toSql(db)
                val original = originalChange.toSql(db)

                assert(contract.all { it in original })
            }
        }
        should("rewrite expand rollback statements when supported database is used") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                db.runInScope {
                    if (!zdChange.generateRollbackStatementsVolatile(db) && zdChange.supportsRollback(db)) {
                        val expandRollback = zdChange.toRollbackSql(db)
                        val originalRollback = originalChange.toRollbackSql(db)

                        assert(expandRollback.none { it in originalRollback })
                    }
                }
            }
        }
        should("not rollback when in contract step") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdContractChangeArb
            ) { db, (zdChange, _) ->
                db.runInScope {
                    assertFailsWith<RollbackImpossibleException> { zdChange.toRollbackSql(db) }
                }
            }
        }
        should("not rewrite rollback statements when zero-downtime is off") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdOffChangeArb
            ) { db, (zdChange, _) ->
                db.runInScope {
                    val offRollback = zdChange.toRollbackSql(db)
                    val originalRollback = zdChange.toRollbackSql(db)

                    assert(offRollback.all { it in originalRollback })
                }
            }
        }
        should("not rewrite rollback statements when unsupported database is used") {
            checkAll(
                TestConstants.unsupportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.supports(db)) {
                    db.runInScope {
                        if (!zdChange.generateRollbackStatementsVolatile(db) && zdChange.supportsRollback(db)) {
                            val expandRollback = zdChange.toRollbackSql(db)
                            val originalRollback = originalChange.toRollbackSql(db)

                            assert(expandRollback.all { it in originalRollback })
                        }
                    }
                }
            }
        }
    }
})