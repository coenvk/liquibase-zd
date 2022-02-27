package liquibase.ext.metadata.view

import liquibase.database.jvm.JdbcConnection
import liquibase.ext.metadata.AbstractMetadataCopyTask
import liquibase.ext.util.KotlinExtensions.forFirst
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

class ViewCopyTask : AbstractMetadataCopyTask<ViewMetadata>() {
    override fun prepareStatement(
        connection: JdbcConnection,
        args: Map<String, Any>
    ): PreparedStatement {
        val query = """
        SELECT
            v.definition
        FROM
            pg_catalog.pg_views v
        WHERE
            v.viewname = '${args["viewName"]}';
        """.trimIndent()
        return connection.prepareStatement(query, Statement.NO_GENERATED_KEYS)
    }

    override fun copy(resultSet: ResultSet, targetObject: ViewMetadata) {
        resultSet.forFirst { rs ->
            targetObject.apply {
                definition = rs.getString(1)
            }
        }
    }
}