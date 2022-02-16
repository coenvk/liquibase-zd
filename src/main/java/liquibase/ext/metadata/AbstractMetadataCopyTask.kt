package liquibase.ext.metadata

import liquibase.Scope
import liquibase.database.Database
import liquibase.database.core.PostgresDatabase
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.DatabaseException
import liquibase.logging.Logger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

internal abstract class AbstractMetadataCopyTask<T> : MetadataCopyTask<T> {
    override fun setUp() = Unit

    protected abstract fun prepareStatement(
        connection: JdbcConnection,
        args: Map<String, Any>
    ): PreparedStatement

    private fun retrieve(
        connection: JdbcConnection,
        args: Map<String, Any>
    ): ResultSet {
        var stmt: PreparedStatement? = null
        try {
            stmt = prepareStatement(connection, args)
            val rs = stmt.executeQuery()
            connection.commit()
            return rs
        } catch (e: SQLException) {
            throw DatabaseException(e)
        } finally {
            stmt?.close()
        }
    }

    protected abstract fun copy(resultSet: ResultSet, targetObject: T)

    @Throws(DatabaseException::class)
    final override fun copy(
        database: Database,
        targetObject: T,
        args: Map<String, Any>
    ): T {
        if (database !is PostgresDatabase) {
            throw DatabaseException("Operation not implemented for non-Postgres databases")
        }
        if (database.connection == null || database.connection.isClosed) {
            LOG.severe("Connection to the database is closed!")
            throw DatabaseException("Connection to the database is closed!")
        }
        setUp()
        val conn = database.connection as? JdbcConnection ?: throw DatabaseException("Requires a JDBC connection")
        copy(retrieve(conn, args), targetObject)
        return targetObject
    }

    companion object {
        val LOG: Logger = Scope.getCurrentScope().getLog(AbstractMetadataCopyTask::class.java)
    }
}