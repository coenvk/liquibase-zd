package liquibase.ext.change

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.checkAll
import liquibase.change.Change
import liquibase.database.Database
import liquibase.exception.DatabaseException
import liquibase.exception.RollbackImpossibleException
import liquibase.ext.base.ZdChange
import liquibase.ext.change.custom.CustomStatement
import liquibase.ext.util.TestConstants
import liquibase.ext.util.TestConstants.runInScope
import liquibase.ext.util.TestKotlinExtensions.assertThrowsIf
import liquibase.sqlgenerator.SqlGeneratorFactory
import org.junit.jupiter.api.assertThrows

class ZdChangeTest : ShouldSpec({
    val generator = SqlGeneratorFactory.getInstance()

    fun ZdChange.containsOriginal(db: Database, originalChange: Change): Boolean =
        generateStatements(db).any {
            originalChange.generateStatements(db).filterIsInstance(it::class.java).any()
        }

    fun ZdChange.containsOriginalRollback(db: Database, originalChange: Change): Boolean =
        generateRollbackStatements(db).any {
            originalChange.generateRollbackStatements(db).filterIsInstance(it::class.java).any()
        }

    fun Change.toSql(db: Database): List<String> = try {
        val stmts = generateStatements(db).filterNot { it is CustomStatement }
        generator.generateSql(stmts.toTypedArray(), db).map { it.toSql() }
    } catch (e: IllegalStateException) {
        emptyList()
    }

    fun Change.toRollbackSql(db: Database): List<String> = try {
        val stmts = generateRollbackStatements(db).filterNot { it is CustomStatement }
        generator.generateSql(stmts.toTypedArray(), db).map { it.toSql() }
    } catch (e: IllegalStateException) {
        emptyList()
    }

    context("Validation") {
        should("rewrite expand statements when supported database is used") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    assertThrowsIf<DatabaseException>(zdChange.generateStatementsVolatile(db)) {
                        val expand = zdChange.toSql(db)
                        val original = originalChange.toSql(db)

                        if (zdChange.containsOriginal(db, originalChange)) {
                            assert(expand.any { it in original })
                        } else assert(expand.none { it in original })
                    }
                }
            }
        }
        should("rewrite contract statements when supported database is used") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdContractChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    assertThrowsIf<DatabaseException>(zdChange.generateStatementsVolatile(db)) {
                        val contract = zdChange.toSql(db)
                        val original = originalChange.toSql(db)

                        if (zdChange.containsOriginal(db, originalChange)) {
                            assert(contract.any { it in original })
                        } else assert(contract.none { it in original })
                    }
                }
            }
        }
        should("not rewrite statements when zero-downtime is off") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdDisabledChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    assertThrowsIf<DatabaseException>(zdChange.generateStatementsVolatile(db)) {
                        val disabled = zdChange.toSql(db)
                        val original = originalChange.toSql(db)

                        if (zdChange.containsOriginal(db, originalChange)) {
                            assert(disabled.any { it in original })
                        } else assert(disabled.none { it in original })
                    }
                }
            }
        }
        should("not rewrite expand statements when unsupported database is used") {
            checkAll(
                TestConstants.unsupportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    assertThrowsIf<DatabaseException>(zdChange.generateStatementsVolatile(db)) {
                        val expand = zdChange.toSql(db)
                        val original = originalChange.toSql(db)

                        assert(expand.all { it in original })
                    }
                }
            }
        }
        should("not rewrite contract statements when unsupported database is used") {
            checkAll(
                TestConstants.unsupportedDatabases,
                TestConstants.zdContractChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    assertThrowsIf<DatabaseException>(zdChange.generateStatementsVolatile(db)) {
                        val contract = zdChange.toSql(db)
                        val original = originalChange.toSql(db)

                        assert(contract.all { it in original })
                    }
                }
            }
        }
        should("rewrite expand rollback statements when supported database is used") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    if (originalChange.supportsRollback(db)) {
                        assertThrowsIf<DatabaseException>(zdChange.generateRollbackStatementsVolatile(db)) {
                            val expandRollback = zdChange.toRollbackSql(db)
                            val originalRollback = originalChange.toRollbackSql(db)

                            if (zdChange.containsOriginalRollback(db, originalChange)) {
                                assert(expandRollback.any { it in originalRollback })
                            } else assert(expandRollback.none { it in originalRollback })
                        }
                    }
                }
            }
        }
        should("not rollback when in contract step") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdContractChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    if (zdChange.generateRollbackStatementsVolatile(db)) {
                        assertThrows<DatabaseException> { zdChange.toRollbackSql(db) }
                    } else assertThrows<RollbackImpossibleException> { zdChange.toRollbackSql(db) }
                }
            }
        }
        should("not rewrite rollback statements when zero-downtime is off") {
            checkAll(
                TestConstants.supportedDatabases,
                TestConstants.zdDisabledChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    if (originalChange.supportsRollback(db)) {
                        assertThrowsIf<DatabaseException>(zdChange.generateRollbackStatementsVolatile(db)) {
                            val disabledRollback = zdChange.toRollbackSql(db)
                            val originalRollback = zdChange.toRollbackSql(db)

                            assert(disabledRollback.all { it in originalRollback })
                        }
                    }
                }
            }
        }
        should("not rewrite rollback statements when unsupported database is used") {
            checkAll(
                TestConstants.unsupportedDatabases,
                TestConstants.zdExpandChangeArb
            ) { db, (zdChange, originalChange) ->
                if (originalChange.validate(db).hasErrors()) return@checkAll
                db.runInScope {
                    if (originalChange.supportsRollback(db)) {
                        assertThrowsIf<DatabaseException>(zdChange.generateRollbackStatementsVolatile(db)) {
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