package org.ascension.addg.gcp.ingestion.read.jdbc;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.KV;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tools for interacting with Derby for JDBC unit testing
 */
public class DatabaseTestHelper {

    private static void createTable(DataSource dataSource, String tableName, List<KV<String, String>> fieldsAndTypes) throws SQLException {
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement()) {
            var sqlString = String.format("CREATE TABLE %s (%s)", tableName, fieldsAndTypes.stream()
                    .map(kv -> kv.getKey() + " " + kv.getValue())
                    .collect(Collectors.joining(", ")));
            statement.execute(sqlString);
        }
    }

    public static void createTable(DataSource dataSource, String tableName) throws SQLException {
        DatabaseTestHelper.createTable(dataSource, tableName, List.of(
                KV.of("ID", "INTEGER"),
                KV.of("NAME", "VARCHAR(10)"),
                KV.of("BOOLEAN_VALUE", "BOOLEAN"),
                KV.of("FLOAT_VALUE", "FLOAT"),
                KV.of("LONG_VALUE", "BIGINT"),
                KV.of("TIMESTAMP_VALUE", "TIMESTAMP")
        ));
    }

    public static void dropTable(DataSource dataSource, String tableName) throws SQLException {
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        }
    }

    public static void configureProps(DataSource dataSource, String user, String password) throws SQLException {
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement()) {
            statement.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.connection.requireAuthentication', 'true')");
            statement.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.sqlAuthorization', 'true')");
            statement.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.authentication.provider', 'BUILTIN')");
            statement.executeUpdate(String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.user.%s', '%s')", user, password));
            statement.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.defaultConnectionMode', 'noAccess')");
            statement.executeUpdate(String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.fullAccessUsers', '%s')", user));
        }
    }

    public static void addInitialData(DataSource dataSource, String tableName, int row_count) throws SQLException {
        var sqlString = String.format("INSERT INTO %s VALUES (?,?,?,?,?,?)", tableName);
        try (var connection = dataSource.getConnection();
             var preparedStatement = connection.prepareStatement(sqlString)) {
            connection.setAutoCommit(false);
            for (int i = 0; i < row_count; i++) {
                preparedStatement.clearParameters();
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, "value_" + i);
                preparedStatement.setBoolean(3, true);
                preparedStatement.setFloat(4, 5.5F);
                preparedStatement.setLong(5, i * 800000L);
                preparedStatement.setTimestamp(6, Timestamp.valueOf(LocalDateTime.of(2023, 1, 2, 12, 5, 0, 0)));
                preparedStatement.executeUpdate();
            }
            connection.commit();
            connection.setAutoCommit(true);
        }
    }

    public static void dropDatabase() throws SQLException {
        JdbcIO.DataSourceConfiguration.create("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;drop=true;user=test_username;password=test_password").buildDatasource().getConnection();
    }
}


