package liquibase.ext.change

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.should
import io.kotest.matchers.types.beInstanceOf
import io.kotest.property.checkAll
import liquibase.change.Change
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.ChangeSet
import liquibase.database.Database
import liquibase.ext.base.ZdChange
import liquibase.ext.util.TestConstants
import liquibase.parser.ChangeLogParser
import liquibase.parser.ChangeLogParserFactory
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.sqlgenerator.SqlGeneratorFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.reflect.KClass

abstract class ZdChangeIntegrationTest(
    originalChangeLogXml: String,
    expandChangeLogXml: String,
    contractChangeLogXml: String,
    changeClass: KClass<out ZdChange>,
) : ShouldSpec({
    val accessor = ClassLoaderResourceAccessor()
    val generator: SqlGeneratorFactory = SqlGeneratorFactory.getInstance()

    val parserFactory: ChangeLogParserFactory = ChangeLogParserFactory.getInstance()
    val xmlParser: ChangeLogParser = parserFactory.getParser(CHANGE_LOG_EXTENSION_XML, accessor)

    fun genSqlString(change: Change, db: Database): String =
        generator.generateSql(change, db).joinToString(separator = "\n") { it.toSql() }.trim()

    fun checkSingleAndCorrectSubType(changeSets: List<ChangeSet>) {
        assertEquals(1, changeSets.size)
        assertEquals(1, changeSets[0].changes.size)
        changeSets.first().changes.first() should beInstanceOf(changeClass)
    }

    beforeAny { SqlGeneratorFactory.reset() }

    should("be of correct subtype when zero-downtime is enabled") {
        checkAll(TestConstants.supportedDatabases) { db ->
            val expandChangeLog = xmlParser.parse(expandChangeLogXml, ChangeLogParameters(db), accessor)
            checkSingleAndCorrectSubType(expandChangeLog.changeSets)
            val contractChangeLog = xmlParser.parse(contractChangeLogXml, ChangeLogParameters(db), accessor)
            checkSingleAndCorrectSubType(contractChangeLog.changeSets)
        }
    }
    should("be of correct subtype when zero-downtime is disabled") {
        checkAll(TestConstants.supportedDatabases) { db ->
            val log = xmlParser.parse(originalChangeLogXml, ChangeLogParameters(db), accessor)
            checkSingleAndCorrectSubType(log.changeSets)
        }
    }
    should("generate equivalent checksum as original") {
        checkAll(TestConstants.supportedDatabases) { db ->
            val originalLog = xmlParser.parse(originalChangeLogXml, ChangeLogParameters(db), accessor)
            val rewriteLog = xmlParser.parse(expandChangeLogXml, ChangeLogParameters(db), accessor)
            val pairs = originalLog.changeSets.zip(rewriteLog.changeSets)
            assertTrue(
                pairs.all { (o, r) ->
                    o.generateCheckSum() == r.generateCheckSum()
                },
                "All rewritten changes should have the same checksum as their original"
            )

            pairs.forEach {
                val checksumsOriginal = it.first.changes.map { c -> c.generateCheckSum() }
                val checksumsRewrite = it.second.changes.map { c -> c.generateCheckSum() }
                assertEquals(checksumsOriginal, checksumsRewrite, "Rewritten checksums should equal their original")
            }
        }
    }
    should("generate same SQL when database is not supported") {
        checkAll(TestConstants.unsupportedDatabases) { db ->
            val originalChangeLog = xmlParser.parse(originalChangeLogXml, ChangeLogParameters(db), accessor)
            val originalChange = originalChangeLog.changeSets.first().changes.first()
            if (originalChange.supports(db)) {
                val expandChangeLog = xmlParser.parse(expandChangeLogXml, ChangeLogParameters(db), accessor)
                val contractChangeLog = xmlParser.parse(contractChangeLogXml, ChangeLogParameters(db), accessor)
                val expandSql = genSqlString(expandChangeLog.changeSets.first().changes.first(), db)
                val contractSql = genSqlString(contractChangeLog.changeSets.first().changes.first(), db)
                val originalSql = genSqlString(originalChange, db)
                assertEquals(1, setOf(expandSql, contractSql, originalSql).size)
            }
        }
    }
    should("not result in validation errors") {
        checkAll(TestConstants.supportedDatabases) { db ->
            val log = xmlParser.parse(expandChangeLogXml, ChangeLogParameters(db), accessor)
            val errors = log.changeSets.first().changes.flatMap { it.validate(db).errorMessages.toSet() }
            assertEquals(0, errors.count())
        }
    }
}) {
    companion object {
        private const val CHANGE_LOG_EXTENSION_XML = ".xml"
    }
}