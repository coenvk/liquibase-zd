package liquibase.ext.change.update

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Exhaustive
import io.kotest.property.checkAll
import io.kotest.property.exhaustive.of
import io.mockk.InternalPlatformDsl.toArray
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import liquibase.change.custom.CustomChangeWrapper
import liquibase.changelog.ChangeLogParameters
import liquibase.database.jvm.JdbcConnection
import liquibase.ext.util.TestConstants
import liquibase.parser.ChangeLogParser
import liquibase.parser.ChangeLogParserFactory
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.jupiter.api.Assertions.assertEquals
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet

class BatchMigrationIntegrationTest : ShouldSpec({
    val accessor = ClassLoaderResourceAccessor()

    val parserFactory: ChangeLogParserFactory = ChangeLogParserFactory.getInstance()
    val xmlParser: ChangeLogParser = parserFactory.getParser(CHANGE_LOG_EXTENSION_XML, accessor)

    should("xml result in expected class") {
        checkAll(TestConstants.supportedDatabases) { db ->
            val xml =
                xmlParser.parse("change/update/batch-migration.xml", ChangeLogParameters(db), accessor)
            assertEquals(1, xml.changeSets.size)
            val changes = xml.changeSets.first().changes
            val customChangeWrapper = changes[0] as CustomChangeWrapper

            assertEquals("customer", customChangeWrapper.getParamValue("tableName"))
            assertEquals("phone", customChangeWrapper.getParamValue("fromColumns"))
            assertEquals("phoneNumber", customChangeWrapper.getParamValue("toColumns"))
            assertEquals("1000", customChangeWrapper.getParamValue("chunkSize"))
            assertEquals(BulkColumnCopyChange::class.java, customChangeWrapper.customChange.javaClass)

            val m = customChangeWrapper.customChange as BulkColumnCopyChange

            val conn = mockk<JdbcConnection>(relaxed = true)
            val stmt = mockk<PreparedStatement>(relaxed = true)
            val md = mockk<DatabaseMetaData>()
            val rs = mockk<ResultSet>()
            val spyDb = spyk(db)
            every { conn.metaData } returns md
            every { md.getBestRowIdentifier(any(), any(), any(), any(), any()) } returns rs
            every { rs.getObject(2) } returns "id"
            every { rs.next() } returns true andThen false
            every { stmt.executeLargeUpdate() } returns 0L // implies done
            every { conn.prepareStatement(any(), any<Int>()) } returns stmt
            every { spyDb.connection } returns conn
            // should make sure that all is set
            customChangeWrapper.generateStatements(spyDb)

            assertEquals("customer", m.tableName)
            assertEquals(1000L, m.chunkSize)
            assertEquals("phone", m.fromColumns)
            assertEquals("phoneNumber", m.toColumns)
        }
    }
}) {
    companion object {
        private const val CHANGE_LOG_EXTENSION_XML = ".xml"
    }
}