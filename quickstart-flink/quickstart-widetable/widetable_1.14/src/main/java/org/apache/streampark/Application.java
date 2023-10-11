package org.apache.streampark;


import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.streampark.bean.ChangeLog;
import org.apache.streampark.map.*;
import org.apache.streampark.msg.Message;

import org.apache.streampark.reduce.DimensionWindowReduceFunction;
import org.apache.streampark.bean.DimensionTable;
import org.apache.streampark.bean.SinkTable;
import org.apache.streampark.utils.*;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.streampark.utils.options.*;


import java.util.Map;


/**
 * @author lysgithub0302
 * @note
 * @date 2023/9/22
 */
public class Application {

    /**
     *
     * --sql-conf business-sql="SELECT cdc.biz_keeper_grade.id,cdc.biz_keeper_grade.bus_opp_num,cdc.biz_keeper_grade.keeper_grade_code,cdc.biz_keeper_grade.keeper_grade,cdc.biz_bus_opp.city_code,cdc.biz_bus_opp.remark,cdc.biz_house.room_num,cdc.biz_house.address FROM cdc.biz_keeper_grade LEFT JOIN cdc.biz_bus_opp ON cdc.biz_keeper_grade.bus_opp_num=cdc.biz_bus_opp.bus_opp_num LEFT JOIN cdc.biz_house ON cdc.biz_bus_opp.house_id=cdc.biz_house.id"

     * --source-conf s1-username=mysqlcdc
     * --source-conf s1-password=Lzslov123!
     * --source-conf s1-hostname=10.30.238.24
     * --source-conf s1-port=13306
     * --source-conf s1-database=cdc
     * --source-conf s1-tables=cdc.biz_keeper_grade,cdc.biz_bus_opp

     * --source-conf s2-username=mysqlcdc
     * --source-conf s2-password=Lzslov123!
     * --source-conf s2-hostname=10.30.238.24
     * --source-conf s2-port=13306
     * --source-conf s2-database=cdc
     * --source-conf s2-tables=cdc.biz_house

     ** --source-conf s[n]-...... More data sources can be configured*

     * --source-conf scan.startup.mode=initial
     * --source-conf scan.snapshot.fetch.size=120000

     * --sink-conf connector=jdbc
     * --sink-conf url=jdbc:mysql://10.30.238.24:13306/widetabledb?useSSL=false&useUnicode=true&characterEncoding=UTF-8
     * --sink-conf table-name=big_table
     * --sink-conf username=mysqlcdc
     * --sink-conf password=Lzslov123!

     * --dim-conf db-type=mysql
     * --dim-conf url-param=useSSL=false&useUnicode=true&characterEncoding=UTF-8
     * --dim-conf username=mysqlcdc
     * --dim-conf password=Lzslov123!
     * --dim-conf hostname=10.30.238.24
     * --dim-conf port=13306
     * --dim-conf database=indexdb

     * --job-conf parallel=6
     * --job-conf window-size=70
     * --job-conf checkpoint-interval=30000
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        Application app = new Application();
        app.run(params);
    }

    public  void run(MultipleParameterTool params) throws JSQLParserException {
        ParamUitl.checkRequiredArgument(params, JobOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, SourceOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, DimensionOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, SqlInfoOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, SinkOptions.PRE_NAME);

        Configuration jobConfig = Configuration.fromMap(ParamUitl.optionalConfigMap(params, JobOptions.PRE_NAME));
        int parallel =  jobConfig.getInteger(JobOptions.PARALLEL);
        int windowSize = jobConfig.getInteger(JobOptions.WINDOW_SIZE);
        int interval = jobConfig.getInteger(JobOptions.CHECKPOINT_INTERVAL);

        Configuration sourceConfig = Configuration.fromMap(ParamUitl.optionalConfigMap(params, SourceOptions.PRE_NAME));
        //解析source
        Map<String,Configuration> sourceConfigMap = ParamUitl.buildSourceConfigMap(sourceConfig);
        String dbTables = sourceConfig.getString(SourceOptions.INCLUDING_TABLES);

        int fetchSize = sourceConfig.getInteger(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        String startupMode = sourceConfig.getString(MySqlSourceOptions.SCAN_STARTUP_MODE);

        StartupOptions startupOptions = StartupOptions.latest();
        if (Constant.INITIAL.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if (Constant.EARLIEST_OFFSET.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.earliest();
        } else if (Constant.LATEST_OFFSET.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.latest();
        }else if (Constant.TIMESTAMP.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.timestamp(sourceConfig.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS));
        }

        Configuration indexConf = Configuration.fromMap(ParamUitl.optionalConfigMap(params, DimensionOptions.PRE_NAME));
        Configuration sqlConf = Configuration.fromMap(ParamUitl.optionalConfigMap(params, SqlInfoOptions.PRE_NAME));
        //解析sql配置
        ParamUitl.praseSql(sqlConf);
        Configuration sinkConfig = Configuration.fromMap(ParamUitl.optionalConfigMap(params, SinkOptions.PRE_NAME));
        String selectFields = sqlConf.getString(SqlInfoOptions.SELECT_FIELDS);

        //生成SinkCreateTableFlinkSql
        String sinkCreateTableFlinkSql = SinkTable.buildSinkCreateTableFlinkSql(selectFields, sinkConfig);

        Configuration conf = new Configuration();
        conf.set(JobOptions.PARALLEL, parallel);
        conf.set(SourceOptions.INCLUDING_TABLES, dbTables);
        conf.addAll(sqlConf);
        conf.addAll(indexConf);
        Map<String, DimensionTable> tablesMap = DimensionTableHelper.buildTableMap(conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallel);
        env.getConfig().setGlobalJobParameters(conf);

        // 设置 30s 的 checkpoint 间隔
        env.enableCheckpointing(interval);

        //多source 合并
        DataStream<Message> messageDataStream = ParamUitl.buildSourceDataStream(parallel, sourceConfigMap, fetchSize, startupOptions, env);
        DataStream<ChangeLog> changelog = messageDataStream
                //开启窗口计算逻辑
                .keyBy(t -> t.getHash_pk())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .reduce(new DimensionWindowReduceFunction())
                //end
                .map(new MessageMysqlSinkMapFunction())
                .flatMap(new MessageMysqlFlatMapFunction());

        System.out.println(env.getExecutionPlan());
        // 2.创建表的执行环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //source table
        tenv.registerDataStream(Constant.CHANGELOG_TABLENAME, changelog);

        //sink table
        tenv.executeSql(sinkCreateTableFlinkSql);

        //dim table
        for(Map.Entry<String,DimensionTable> item :tablesMap.entrySet()){
            String createTempTableSql = item.getValue().getCreate_table_flink_sql();
            //System.out.println(createTempTableSql);
            tenv.executeSql(createTempTableSql);
        }

        // insert sql
        String insert_flink_sql = conf.getString(SqlInfoOptions.INSERT_FLINK_SQL);

        tenv.executeSql(insert_flink_sql);
    }


}