package org.apache.streampark.test.widetable;


import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.streampark.Application;
import org.apache.streampark.utils.Constant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;


public class MySqlWideTableITCase extends MySqlWideTableITCaseBase {

    private static final String BUSINESS_DATABASE_NAME = "cdc";
    private static final String DEMENSION_DATABASE_NAME = "demension";
    private static final String WIDETABLEDB_DATABASE_NAME = "widetabledb";
    private static final String BUSINESS_SQL = "SELECT cdc.biz_keeper_grade.id,cdc.biz_keeper_grade.bus_opp_num,cdc.biz_keeper_grade.keeper_grade_code,cdc.biz_keeper_grade.keeper_grade,cdc.biz_bus_opp.city_code,cdc.biz_bus_opp.remark,cdc.biz_house.room_num,cdc.biz_house.address FROM cdc.biz_keeper_grade LEFT JOIN cdc.biz_bus_opp ON cdc.biz_keeper_grade.bus_opp_num=cdc.biz_bus_opp.bus_opp_num LEFT JOIN cdc.biz_house ON cdc.biz_bus_opp.house_id=cdc.biz_house.id";
    private static final String S1_TABLES = "cdc.biz_keeper_grade,cdc.biz_bus_opp";
    private static final String S2_TABLES = "cdc.biz_house";
    private static final String BIG_TABLE = "big_table";
    private static  List<String> TABLES = new ArrayList<>();

    static{
        TABLES.addAll(Arrays.asList(S1_TABLES.split(Constant.DOU_HAO)));
        TABLES.addAll(Arrays.asList(S2_TABLES.split(Constant.DOU_HAO)));
    }

    @BeforeAll
    public static void startContainers() throws JSQLParserException {
        MYSQL_CONTAINER.withSetupSQL("mysql/wide_table_setup.sql");
        start();
        runApp();
    }

    private static void runApp() throws JSQLParserException {
        MultipleParameterTool params = buildParam();
        Application app = new Application();
        app.run(params);
    }

    private static MultipleParameterTool buildParam() {
        List<String> args = new ArrayList<>();

        Map<String,String> sqlConf = new HashMap<String, String>();
        sqlConf.put("business-sql",BUSINESS_SQL);
        args.addAll(mapToArgs("--sql-conf", sqlConf));

        Map<String,String> sourceConf = new HashMap<String, String>();
        sourceConf.put("s1-tables", S1_TABLES);
        sourceConf.put("s1-database", BUSINESS_DATABASE_NAME);
        sourceConf.put("s1-port",PORT);
        sourceConf.put("s1-hostname",HOSTNAME);
        sourceConf.put("s1-password",PASSWORD);
        sourceConf.put("s1-username",USER);

        sourceConf.put("s2-tables", S2_TABLES);
        sourceConf.put("s2-database", BUSINESS_DATABASE_NAME);
        sourceConf.put("s2-port",PORT);
        sourceConf.put("s2-hostname",HOSTNAME);
        sourceConf.put("s2-password",PASSWORD);
        sourceConf.put("s2-username",USER);

        sourceConf.put("scan.startup.mode","initial");
        sourceConf.put("scan.snapshot.fetch.size","120000");
        args.addAll(mapToArgs("--source-conf", sourceConf));

        Map<String,String> sinkConf = new HashMap<String, String>();
        sinkConf.put("password", PASSWORD);
        sinkConf.put("username", USER);
        sinkConf.put("table-name", BIG_TABLE);
        sinkConf.put("url", "jdbc:mysql://"+HOSTNAME+":"+ PORT + "/" + WIDETABLEDB_DATABASE_NAME + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8");
        sinkConf.put("connector", "jdbc");
        args.addAll(mapToArgs("--sink-conf", sinkConf));

        Map<String,String> dimConf = new HashMap<String, String>();
        dimConf.put("database", DEMENSION_DATABASE_NAME);
        dimConf.put("port", PORT);
        dimConf.put("hostname", HOSTNAME);
        dimConf.put("password", PASSWORD);
        dimConf.put("username", USER);
        dimConf.put("url-param", "useSSL=false&useUnicode=true&characterEncoding=UTF-8");
        dimConf.put("db-type", "mysql");
        args.addAll(mapToArgs("--dim-conf", dimConf));

        Map<String,String> jobConf = new HashMap<String, String>();
        jobConf.put("parallel","6");
        jobConf.put("window-size","70");
        jobConf.put("checkpoint-interval","10000");
        args.addAll(mapToArgs("--job-conf", jobConf));


        MultipleParameterTool params = MultipleParameterTool.fromArgs(args.toArray(args.toArray(new String[0])));
        return params;
    }


    @Test
    public void testDemensionTable() throws SQLException {
        sleep10s();
        for(String tableName: TABLES){
            try (Statement statement = getStatement()) {
                System.out.println(tableName);
                checkTable(statement,
                        BUSINESS_DATABASE_NAME,
                        "select count(*) as num from " + tableName,
                        DEMENSION_DATABASE_NAME,
                        "select count(*) as num from " + tableName.replace(Constant.POINT, Constant.XIALINE));
            }
        }
    }

    @Test
    public void testWidetable() throws SQLException {

        sleep10s();
        try (Statement statement = getStatement()) {
            //cdc tables
            checkTable(statement,
                    BUSINESS_DATABASE_NAME,
                    "select count(*) as num from ( "+BUSINESS_SQL+" ) t",
                    WIDETABLEDB_DATABASE_NAME,
                    "select count(*) as num from " + BIG_TABLE);
        }
    }

    private void checkTable(Statement statement,String sourceDB, String sourceSql, String targetDB, String targetSql) throws SQLException {
        //source tables
        statement.execute("use " + sourceDB);
        ResultSet rs = statement.executeQuery(sourceSql);

        int s_num = 0;
        while (rs.next()) {
            int count = rs.getInt(1);
            s_num = count;
        }

        //target tables
        statement.execute("use " + targetDB);
        rs = statement.executeQuery(targetSql);
        int t_num = 0;
        while (rs.next()) {
            int count = rs.getInt(1);
            t_num = count;
        }
        assertThat(t_num).isEqualTo(s_num);
    }

    private void sleep10s() {
        try {
            Thread.sleep(10 * 1000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
