package liquibase.ext.change.update

import io.kotest.property.Arb
import io.kotest.property.Exhaustive
import io.kotest.property.RandomSource
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.filter
import io.kotest.property.arbitrary.filterNot
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.next
import io.kotest.property.arbitrary.orNull
import io.kotest.property.exhaustive.azstring
import io.kotest.property.exhaustive.exhaustive
import java.sql.RowIdLifetime

object BatchMigrationGenerator {
    val identifierGen = { min: Int -> Exhaustive.azstring(min..16).toArb() }

    val rowIdLifeTimeInvalidGenerator = listOf(
        RowIdLifetime.ROWID_UNSUPPORTED,
        RowIdLifetime.ROWID_VALID_OTHER,
        RowIdLifetime.ROWID_VALID_SESSION,
        RowIdLifetime.ROWID_VALID_TRANSACTION
    ).exhaustive()

    val validMigrationGenerator = arbitrary { rs: RandomSource ->
        val change = BatchColumnMigrationChange()

        val colCount = Arb.int(1, 5).next(rs)
        val colGen = fixedColumnListNoDupsGenerator(colCount, colCount)

        change.tableName = identifierGen(1).next(rs)
        change.chunkSize = Arb.long(1L, 10000L).next(rs)
        val from = colGen.next(rs)
        val fromSet = from.toSet()
        change.fromColumns = from.toColumnList()

        // Make sure we do not have overlapping or crossing columns between from and to
        val to = colGen.filterNot { l -> fromSet.any { it in l.toSet() } }.next(rs)
        change.toColumns = to.toColumnList()

        change
    }

    val validMigrationWithSleepsGenerator = arbitrary { rs: RandomSource ->
        val mig = validMigrationGenerator.next(rs)
        mig.sleepTime = Arb.long(0L, 10000L).orNull().next(rs)
        mig
    }

    private val sampleMigrationGenerator = arbitrary { rs: RandomSource ->
        val change = BatchColumnMigrationChange()
        change.tableName = identifierGen(1).orNull().next(rs)
        change.chunkSize = Arb.long(-100L, 10000L).orNull().next(rs)
        val upperBound = Arb.int(0, 5).next(rs)
        val minBound = Arb.int(0, 5).filter { it <= upperBound }.next(rs)
        change.fromColumns = fixedColumnStringSequenceGenerator(minBound, upperBound).orNull().next(rs)
        change.toColumns = fixedColumnStringSequenceGenerator(minBound, upperBound).orNull().next(rs)
        change.sleepTime = Arb.long(-100L, 10000L).orNull().next(rs)
        change
    }

    val invalidMigrationGenerator = sampleMigrationGenerator.filter { c: BatchColumnMigrationChange ->
        val simplePredicate = c.fromColumns.isNullOrEmpty() ||
                c.toColumns.isNullOrEmpty() || (c.chunkSize ?: -1L) <= 0L || c.sleepTime?.let { it < 0L } ?: false
        if (simplePredicate) return@filter true
        else {
            val from = c.fromColumns!!.split(",")
            val to = c.toColumns!!.split(",").toSet()
            // check whether from and to columns are equal somewhere or crossing
            // check whether any to column is in primary keys
            from.size != to.size || from.any { it in to }
        }
    }

    private fun List<String>.toColumnList(): String = joinToString(separator = ",") { it }

    private val fixedColumnListGenerator = { lowerBound: Int, inclusiveUpperBound: Int ->
        Arb.list(identifierGen(1), IntRange(lowerBound, inclusiveUpperBound))
    }

    private val fixedColumnListNoDupsGenerator = { lowerBound: Int, inclusiveUpperBound: Int ->
        fixedColumnListGenerator(lowerBound, inclusiveUpperBound).filterNot { l ->
            l.toSet().size != l.size
        }
    }
    private val fixedColumnStringSequenceGenerator = { lowerBound: Int, inclusiveUpperBound: Int ->
        fixedColumnListGenerator(lowerBound, inclusiveUpperBound).map { l -> l.joinToString(",") { it } }
    }
}