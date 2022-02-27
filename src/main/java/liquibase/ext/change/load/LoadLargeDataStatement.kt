package liquibase.ext.change.load

import liquibase.change.ColumnConfig
import liquibase.statement.AbstractSqlStatement
import liquibase.util.csv.CSVReader

class LoadLargeDataStatement(
    val catalogName: String? = null,
    val schemaName: String? = null,
    val tableName: String,
    val fileName: String,
    val columns: List<ColumnConfig>
) : AbstractSqlStatement() {
    var format: String = "csv"
    var delimiter: String = CSVReader.DEFAULT_SEPARATOR.toString()
    var quote: String = CSVReader.DEFAULT_QUOTE_CHARACTER.toString()
    var escape: String? = null
    var encoding: String = "utf-8"
    var nullString: String = "\\N"
    var freeze: Boolean = false
    var header: Boolean = false

    override fun continueOnError(): Boolean = false
}