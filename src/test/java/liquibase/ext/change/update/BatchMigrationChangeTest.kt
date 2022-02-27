package liquibase.ext.change.update

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.checkAll
import io.kotest.property.exhaustive.exhaustive
import io.kotest.property.forAll
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.CustomChangeException
import liquibase.ext.util.KotlinExtensions.getAll
import liquibase.ext.util.TestConstants
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.assertDoesNotThrow
import java.sql.*
import liquibase.ext.change.update.BatchMigrationGenerator as gen

class BatchMigrationChangeTest : ShouldSpec({
    context("Validation") {
        should("result in errors when not all arguments meet the constraints") {
            forAll(TestConstants.supportedDatabases, gen.invalidMigrationGenerator) { db, c ->
                c.validate(db).hasErrors()
            }
        }
        should("pass when all arguments do not meet the constraints but non-supported database is used") {
            forAll(TestConstants.unsupportedDatabases, gen.invalidMigrationGenerator) { db, c ->
                !c.validate(db).hasErrors()
            }
        }
        should("pass when all arguments meet the constraints") {
            forAll(TestConstants.supportedDatabases, gen.validMigrationWithSleepsGenerator) { db, c ->
                !c.validate(db).hasErrors()
            }
        }
        should("pass when all arguments meet the constraints but non-supported database is used") {
            forAll(TestConstants.unsupportedDatabases, gen.validMigrationWithSleepsGenerator) { db, c ->
                !c.validate(db).hasErrors()
            }
        }
    }
    context("Execution") {
        should("do nothing on unsupported databases") {
            checkAll(TestConstants.unsupportedDatabases, gen.validMigrationGenerator) { db, c ->
                val conn = mockk<JdbcConnection>()
                val spyDb = spyk(db)
                every { spyDb.connection } returns conn

                c.execute(spyDb)

                verify(exactly = 0) { conn.prepareStatement(any(), any<Int>()) }
                verify(exactly = 0) { conn.prepareStatement(any()) }
                verify(exactly = 0) { conn.commit() }
            }
        }
        should("close statements as many times as commits when execution goes fine") {
            checkAll(TestConstants.supportedDatabases, gen.validMigrationGenerator) { db, c ->
                val conn = mockk<JdbcConnection>(relaxed = true)
                val md = mockk<DatabaseMetaData>()
                val rs = mockk<ResultSet>()
                val stmt = mockk<PreparedStatement>()
                val spyDb = spyk(db)
                val expectedUpdates = listOf(c.chunkSize!!, c.chunkSize!!, 0L)

                every { spyDb.connection } returns conn
                every { conn.isClosed } returns false
                every { conn.metaData } returns md
                every { md.getBestRowIdentifier(any(), any(), any(), any(), any()) } returns rs
                every { rs.getObject(2) } returns "id"
                every { rs.next() } returns true andThen false
                every { stmt.close() } returns Unit
                every { stmt.executeLargeUpdate() } returnsMany expectedUpdates
                every { conn.prepareStatement(any(), Statement.RETURN_GENERATED_KEYS) } returns stmt

                c.execute(spyDb)

                verify(exactly = expectedUpdates.size) { conn.commit() }
                verify(exactly = expectedUpdates.size) { stmt.close() }
            }
        }
        should("close statements as many times as they are prepared when exceptions may occur") {
            checkAll(TestConstants.supportedDatabases, gen.validMigrationGenerator) { db, c ->
                val conn = mockk<JdbcConnection>(relaxed = true)
                val md = mockk<DatabaseMetaData>()
                val rs = mockk<ResultSet>()
                val stmt = mockk<PreparedStatement>()
                val spyDb = spyk(db)
                val expectedUpdates = listOf(c.chunkSize!!, c.chunkSize!!, 0L)

                every { spyDb.connection } returns conn
                every { conn.isClosed } returns false
                every { conn.metaData } returns md
                every { md.getBestRowIdentifier(any(), any(), any(), any(), any()) } returns rs
                every { rs.getObject(2) } returns "id"
                every { rs.next() } returns true andThen false
                every { stmt.close() } returns Unit
                every { stmt.executeLargeUpdate() } returnsMany expectedUpdates
                every { conn.commit() } returns Unit andThen Unit andThenThrows SQLException("Anything")
                every { conn.prepareStatement(any(), Statement.RETURN_GENERATED_KEYS) } returns stmt

                try {
                    c.execute(spyDb)
                } catch (e: CustomChangeException) {
                }

                verify(exactly = expectedUpdates.size) { conn.commit() }
                verify(exactly = expectedUpdates.size) { stmt.close() }
            }
        }
        should("stop when the connection is not set") {
            checkAll(TestConstants.supportedDatabases) { db ->
                val spyDb = spyk(db)
                every { spyDb.connection } returns null

                assertThrows(CustomChangeException::class.java) {
                    BulkColumnCopyChange().execute(spyDb)
                }
            }
        }
        should("stop when the connection is closed") {
            checkAll(TestConstants.supportedDatabases) { db ->
                val conn = mockk<JdbcConnection>()
                val spyDb = spyk(db)
                every { conn.isClosed } returns true
                every { spyDb.connection } returns null

                assertThrows(CustomChangeException::class.java) {
                    BulkColumnCopyChange().execute(spyDb)
                }
            }
        }
        should("update in the expected chunks") {
            checkAll(
                TestConstants.supportedDatabases,
                listOf(0L, 1L, 2L, 50000L).exhaustive(),
                gen.validMigrationGenerator
            ) { db, allRowCount, migration ->
                val conn = mockk<JdbcConnection>()
                val md = mockk<DatabaseMetaData>()
                val rs = mockk<ResultSet>()
                val stmt = mockk<PreparedStatement>()
                val spyDb = spyk(db)

                // We split the batch in chunks where we need one extra update to make sure we migrated all
                val requiredUpdates = (allRowCount / migration.chunkSize!! + 1).toInt()
                // Last result should return 0
                var updateResults = listOf(0L)
                repeat(requiredUpdates - 1) {
                    updateResults = listOf(migration.chunkSize!!) + updateResults
                }

                every { spyDb.connection } returns conn
                every { conn.isClosed } returns false
                every { conn.metaData } returns md
                every { md.getBestRowIdentifier(any(), any(), any(), any(), any()) } returns rs
                every { rs.getObject(2) } returns "id"
                every { rs.next() } returns true andThen false
                every { stmt.executeLargeUpdate() } returnsMany updateResults
                every { conn.prepareStatement(any(), Statement.RETURN_GENERATED_KEYS) } returns stmt
                every { conn.commit() } returns Unit
                every { stmt.close() } returns Unit

                migration.execute(spyDb)

                verify(exactly = requiredUpdates) { conn.prepareStatement(any(), Statement.RETURN_GENERATED_KEYS) }
                verify(exactly = requiredUpdates) { stmt.executeLargeUpdate() }
                verify(exactly = requiredUpdates) { conn.commit() }
            }
        }
        should("only construct expected update statements") {
            checkAll(TestConstants.supportedDatabases, gen.validMigrationGenerator) { db, migration ->
                val conn = mockk<JdbcConnection>()
                val md = mockk<DatabaseMetaData>()
                val rs = mockk<ResultSet>()
                val stmt = mockk<PreparedStatement>()
                val spyDb = spyk(db)

                val n = migration.chunkSize!!

                val slot = mutableListOf<String>()
                // Should always be 5 updates
                val tail = kotlin.math.max(n / 2L, 1L)

                every { spyDb.connection } returns conn
                every { conn.isClosed } returns false
                every { conn.metaData } returns md
                every { md.getBestRowIdentifier(any(), any(), any(), any(), any()) } returns rs
                every { rs.getObject(2) } returns "id"
                every { rs.next() } returns true andThen false
                every { stmt.executeLargeUpdate() } returnsMany listOf(n, n, n, tail, 0L)
                every { conn.prepareStatement(capture(slot), Statement.RETURN_GENERATED_KEYS) } returns stmt
                every { conn.commit() } returns Unit
                every { stmt.close() } returns Unit

                migration.execute(spyDb)
                verify(exactly = 5) { conn.prepareStatement(any(), Statement.RETURN_GENERATED_KEYS) }
            }
        }
    }
})