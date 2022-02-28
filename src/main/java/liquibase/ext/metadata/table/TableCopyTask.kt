package liquibase.ext.metadata.table

import liquibase.database.jvm.JdbcConnection
import liquibase.ext.metadata.AbstractMetadataCopyTask
import liquibase.ext.util.KotlinExtensions.forEach
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

class TableCopyTask : AbstractMetadataCopyTask<TableMetadata>() {
    override fun prepareStatement(
        connection: JdbcConnection,
        args: Map<String, Any>
    ): PreparedStatement {
        val tableName = args["tableName"]
        val query = """
        SELECT
            a.attname,
            i.indisprimary
        FROM
            pg_catalog.pg_attribute a
            LEFT JOIN pg_catalog.pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE
            a.attnum > 0
            AND NOT a.attisdropped
            AND a.attrelid = '${tableName}'::regclass;
        """.trimIndent()
        return connection.prepareStatement(query, Statement.NO_GENERATED_KEYS)
    }

    override fun copy(resultSet: ResultSet, targetObject: TableMetadata) {
        resultSet.forEach { rs ->
            val columnName = rs.getString(1)
            targetObject.columnNames.add(columnName)
            if (rs.getBoolean(2)) {
                targetObject.primaryColumnNames.add(columnName)
            }
        }
    }
}