package liquibase.ext.change.load

import liquibase.Scope
import liquibase.change.*
import liquibase.database.Database
import liquibase.exception.LiquibaseException
import liquibase.exception.UnexpectedLiquibaseException
import liquibase.exception.ValidationErrors
import liquibase.ext.util.KotlinExtensions.mapIndexedIf
import liquibase.ext.util.KotlinExtensions.throwIf
import liquibase.logging.Logger
import liquibase.statement.SqlStatement
import liquibase.util.StreamUtil
import liquibase.util.StringUtil
import liquibase.util.csv.CSVReader
import java.io.File
import java.io.IOException
import java.util.*

@DatabaseChange(
    name = "loadLargeData",
    description = """Loads data from a CSV file into an existing table. A value of NULL in a cell will be converted to a database NULL rather than the string 'NULL'.
Lines starting with # (hash) sign are treated as comments. You can change comment pattern by specifying 'commentLineStartsWith' attribute.To disable comments set 'commentLineStartsWith' to empty value'

If the data type for a load column is set to NUMERIC, numbers are parsed in US locale (e.g. 123.45).
Date/Time values included in the CSV file should be in ISO format http://en.wikipedia.org/wiki/ISO_8601 in order to be parsed correctly by Liquibase. Liquibase will initially set the date format to be 'yyyy-MM-dd'T'HH:mm:ss' and then it checks for two special cases which will override the data format string.

If the string representing the date/time includes a '.', then the date format is changed to 'yyyy-MM-dd'T'HH:mm:ss.SSS'
If the string representing the date/time includes a space, then the date format is changed to 'yyyy-MM-dd HH:mm:ss'
Once the date format string is set, Liquibase will then call the SimpleDateFormat.parse() method attempting to parse the input string so that it can return a Date/Time. If problems occur, then a ParseException is thrown and the input string is treated as a String for the INSERT command to be generated.
If UUID type is used UUID value is stored as string and NULL in cell is supported.""",
    priority = ChangeMetaData.PRIORITY_DEFAULT,
    appliesTo = ["table"]
)
class LoadLargeDataChange : AbstractTableChange() {
    var file: String? = null
        @DatabaseChangeProperty(
            description = "CSV file to load",
            exampleValue = "com/example/users.csv",
            requiredForDatabase = [ChangeParameterMetaData.ALL]
        )
        get
    var encoding = "utf-8"
        @DatabaseChangeProperty(
            exampleValue = "UTF-8",
            supportsDatabase = [ChangeParameterMetaData.ALL],
            description = "Encoding of the CSV file (defaults to UTF-8)"
        )
        get
    var separator = CSVReader.DEFAULT_SEPARATOR.toString()
        @DatabaseChangeProperty(
            exampleValue = ",",
            supportsDatabase = [ChangeParameterMetaData.ALL],
            description = "Character separating the fields. Default: " + CSVReader.DEFAULT_SEPARATOR
        )
        get
    var quote = CSVReader.DEFAULT_QUOTE_CHARACTER.toString()
        @DatabaseChangeProperty(
            exampleValue = "'",
            supportsDatabase = [ChangeParameterMetaData.ALL],
            description = "The quote character for string fields containing the separator character. " +
                    "Default: " + CSVReader.DEFAULT_QUOTE_CHARACTER
        )
        get
    var columnNames: String? = null
        @DatabaseChangeProperty(
            supportsDatabase = [ChangeParameterMetaData.ALL]
        )
        get

    private val columns: MutableList<ColumnConfig> = arrayListOf()

    override fun validate(database: Database?): ValidationErrors {
        val validationErrors = ValidationErrors()
        validationErrors.checkRequiredField("tableName", tableName)
        validationErrors.checkRequiredField("file", file)
        return validationErrors
    }

    override fun supports(database: Database?): Boolean = true
    override fun generateStatementsVolatile(database: Database?): Boolean = true
    override fun generateRollbackStatementsVolatile(database: Database?): Boolean = true

    override fun checkStatus(database: Database?): ChangeStatus =
        ChangeStatus().unknown("Cannot check loadData status")

    override fun getConfirmationMessage(): String =
        String.format(coreBundle.getString("loaddata.successful"), file, getTableName())

    override fun generateStatements(database: Database?): Array<SqlStatement> {
        var reader: CSVReader? = null
        try {
            reader = getCSVReader()

            if (reader == null) {
                throw UnexpectedLiquibaseException("Unable to read file " + this.file)
            }

            val headers =
                reader.readNext() ?: throw UnexpectedLiquibaseException("Data file $file was empty")

            addColumnsFromHeaders(headers)

            val stmt = LoadLargeDataStatement(
                catalogName,
                schemaName,
                tableName,
                File(file!!).absolutePath,
                columns,
            )
            stmt.header = true
            stmt.encoding = encoding
            stmt.quote = quote
            stmt.delimiter = separator
            return arrayOf(stmt)
        } catch (ule: UnexpectedLiquibaseException) {
            return if (changeSet != null && changeSet.failOnError != null && !changeSet.failOnError) {
                LOG.info(
                    "Change set " + changeSet.toString(false) +
                            " failed, but failOnError was false.  Error: " + ule.message
                )
                arrayOf()
            } else {
                throw ule
            }
        } finally {
            reader?.close()
        }
    }

    private fun addColumnsFromHeaders(headers: Array<String>) {
        val columnNames = this.columnNames?.split(',')
        headers
            .throwIf(UnexpectedLiquibaseException("All column names must be present in the CSV header ${headers.joinToString()}.")) {
                if (columnNames != null) it.size != columnNames.size
                else false
            }
            .mapIndexedIf(columnNames != null) { i, _ ->
                columnNames!![i]
            }.forEach { columnConfigFromName(it) }
    }

    private fun columnConfigFromName(name: String): ColumnConfig {
        if (null == StringUtil.trimToNull(name)) {
            throw UnexpectedLiquibaseException("Unreferenced unnamed column is not supported")
        }
        val cfg = ColumnConfig()
        columns.add(cfg)
        cfg.name = name
        return cfg
    }

    @Throws(IOException::class, LiquibaseException::class)
    private fun getCSVReader(): CSVReader? {
        val resourceAccessor = Scope.getCurrentScope().resourceAccessor
            ?: throw UnexpectedLiquibaseException("No file resourceAccessor specified for $file")
        val stream = resourceAccessor.openStream(null, file) ?: return null
        val streamReader = StreamUtil.readStreamWithReader(stream, encoding)
        val quote: Char = if (StringUtil.trimToEmpty(this.quote).isEmpty()) {
            // hope this is impossible to have a field surrounded with non ascii char 0x01
            '\u0001'
        } else {
            this.quote[0]
        }
        val delim = if (separator == "\\t") '\t' else separator[0]
        LOG.info("Using CSV reader with encoding=$encoding, quote=$quote, separator=${delim}")
        return CSVReader(streamReader, delim, quote)
    }

    companion object {
        private val LOG: Logger = Scope.getCurrentScope().getLog(LoadLargeDataChange::class.java)
        private val coreBundle: ResourceBundle = ResourceBundle.getBundle("liquibase/i18n/liquibase-core")
    }
}