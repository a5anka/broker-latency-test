package org.wso2.mb.test;

import com.ibatis.common.jdbc.ScriptRunner;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;

public class DbUtils {
    private static final String DATABASE_URL = "jdbc:derby:memory:mbDB";
    private static final String path = "../src/main/resources/derby-mb.sql";


    public static void setupDB() throws SQLException, IOException {

        Connection connection = DriverManager.getConnection(DATABASE_URL + ";create=true");

        ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);
        scriptRunner.runScript(new BufferedReader(new FileReader(path)));

        connection.close();
    }

    public static DataSource getDataSource() {
        HikariConfig hikariDataSourceConfig = new HikariConfig();
        hikariDataSourceConfig.setJdbcUrl(DATABASE_URL);
        hikariDataSourceConfig.setAutoCommit(false);
        return new HikariDataSource(hikariDataSourceConfig);
    }
}
