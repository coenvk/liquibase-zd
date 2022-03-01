package liquibase.ext.metadata.column

import liquibase.database.jvm.JdbcConnection
import liquibase.ext.metadata.AbstractMetadataCopyTask
import liquibase.ext.util.KotlinExtensions.forFirst
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

class ColumnCopyTask : AbstractMetadataCopyTask<ColumnMetadata>() {
    override fun prepareStatement(
        connection: JdbcConnection,
        args: Map<String, Any>
    ): PreparedStatement {
        val tableName = args["tableName"]
        val columnName = args["columnName"]
        val query = """
        WITH t AS (SELECT c.oid, c.relname
                FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE pg_catalog.pg_table_is_visible(c.oid))
        SELECT
            pg_catalog.format_type(a.atttypid, a.atttypmod),
            a.attnotnull,
            pg_catalog.pg_get_expr(d.adbin, d.adrelid),
            c.contype,
            c.conname,
            c.condeferrable,
            c.condeferred,
            c.convalidated,
            c.confupdtype,
            c.confdeltype,
            c_base.columns,
            c_ref.relname,
            c_ref.referenced_columns
        FROM
            pg_catalog.pg_attribute a
            LEFT JOIN t tab ON tab.oid = a.attrelid
            LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
            LEFT JOIN pg_constraint c ON c.conrelid = a.attrelid AND a.attnum = ANY(c.conkey)
            LEFT JOIN (SELECT t.relname, cc.conname, t.oid, string_agg(distinct aa.attname, ',') AS columns
                FROM pg_attribute aa, t, pg_constraint cc
                WHERE aa.attnum > 0
                    AND cc.conrelid = t.oid
                    AND aa.attrelid = t.oid
                    AND t.relname = '${tableName}'
                    AND aa.attname = '${columnName}'
                    AND aa.attnum IN (SELECT unnest(cc.conkey))
                GROUP BY t.oid, t.relname, cc.conname) AS c_base ON c_base.oid = a.attrelid AND c_base.conname = c.conname
            LEFT JOIN (SELECT t.relname, cc.conname, t.oid, string_agg(distinct aa.attname, ',') AS referenced_columns
                FROM pg_attribute aa, t, pg_constraint cc
                WHERE aa.attnum > 0
                    AND cc.confrelid = t.oid
                    AND aa.attrelid = t.oid
                    AND aa.attnum IN (SELECT unnest(cc.confkey))
                GROUP BY t.oid, t.relname, cc.conname) AS c_ref ON c_ref.oid = c.confrelid AND c_ref.conname = c.conname
        WHERE
            a.attnum > 0
            AND NOT a.attisdropped
            AND a.attrelid = tab.oid
            AND tab.relname = '${tableName}'
            AND a.attname = '${columnName}';
        """.trimIndent()
        return connection.prepareStatement(query, Statement.NO_GENERATED_KEYS)
    }

    override fun copy(resultSet: ResultSet, targetObject: ColumnMetadata) {
        resultSet.forFirst { rs ->
            targetObject.apply {
                type = rs.getString(1)
                isNullable = !rs.getBoolean(2)
                defaultValue = rs.getString(3)

                when (rs.getString(4)) {
                    "f" -> {
                        constraints.add(
                            ForeignKeyConstraintMetadata(
                                name = rs.getString(5),
                                isDeferrable = rs.getBoolean(6),
                                isDeferred = rs.getBoolean(7),
                                validate = rs.getBoolean(8),
                                onUpdate = ForeignKeyConstraintMetadata.Action.from(rs.getString(9)),
                                onDelete = ForeignKeyConstraintMetadata.Action.from(rs.getString(10)),
                                baseColumnNames = rs.getString(11),
                                referencedTableName = rs.getString(12),
                                referencedColumnNames = rs.getString(13),
                            )
                        )
                    }
                    "p" -> {
                        constraints.add(
                            PrimaryKeyConstraintMetadata(
                                name = rs.getString(5),
                                validate = rs.getBoolean(8),
                                columnNames = rs.getString(11)
                            )
                        )
                    }
                    "u" -> {
                        constraints.add(
                            UniqueConstraintMetadata(
                                name = rs.getString(5),
                                isDeferrable = rs.getBoolean(6),
                                isDeferred = rs.getBoolean(7),
                                validate = rs.getBoolean(8),
                                columnNames = rs.getString(11)
                            )
                        )
                    }
                    else -> {/* Other constraints are not implemented yet */}
                }
            }
        }
    }
}