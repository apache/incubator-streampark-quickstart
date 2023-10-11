package org.apache.streampark.test.widetable;


import org.apache.streampark.test.utils.MySqlContainer;
import org.apache.streampark.test.utils.MySqlVersion;
import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.*;
import java.util.stream.Stream;


public class MySqlWideTableITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlWideTableITCaseBase.class);

    protected static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    protected static final String USER = "mysqlcdc";
    protected static final String PASSWORD = "mysqlpw";
    protected static  String HOSTNAME;
    protected static  String PORT;
    protected static  String JDBCURL;

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }


    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return (MySqlContainer)
                new MySqlContainer(version)
                        .withConfigurationOverride("mysql/my.cnf")
                        .withUsername(USER)
                        .withPassword(PASSWORD)
                        .withEnv("TZ", "America/Los_Angeles")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    protected static void start() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
        praseMysqlInfo();
    }

    private static void praseMysqlInfo() {
        HOSTNAME = MYSQL_CONTAINER.getHost();
        PORT = String.valueOf(MYSQL_CONTAINER.getDatabasePort());
        JDBCURL = MYSQL_CONTAINER.getJdbcUrl();
    }

    protected Statement getStatement() throws SQLException {
        Connection conn =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword());
        return conn.createStatement();
    }

    protected static List<String> mapToArgs(String argKey, Map<String, String> map) {
        List<String> args = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            args.add(argKey);
            args.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }
        return args;
    }

}
