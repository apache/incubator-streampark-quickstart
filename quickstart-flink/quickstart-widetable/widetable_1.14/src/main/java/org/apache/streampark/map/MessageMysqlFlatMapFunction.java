package org.apache.streampark.map;


import org.apache.streampark.msg.Message;
import org.apache.streampark.bean.DimensionTable;
import org.apache.streampark.bean.ChangeLog;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.streampark.utils.DimensionTableHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lysgithub0302
 * @note
 * @date 2023/8/10
 */
@Slf4j
public class MessageMysqlFlatMapFunction extends RichFlatMapFunction<Message, ChangeLog> {
    private static final long serialVersionUID = 1L;

    private transient Connection connection;

    private transient static Map<String, DimensionTable> tablesMap;

    private transient static final int BATCH_SIZE = 100;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Configuration globConf = (Configuration) globalParams;

            this.tablesMap = DimensionTableHelper.buildTableMap(globConf);

            if (null == connection) {

                connection = DimensionTableHelper.getConnection();
                //开始建表
                createTable(connection);
            }

        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot create connection to Mysql.", e);
        }
        System.out.println("main key connection open");
    }

    public void createTable(Connection connection) throws SQLException {
        // 创建Statement对象
        Statement statement = connection.createStatement();
        for(Map.Entry<String, DimensionTable> table : tablesMap.entrySet()){
            String creatTableSql = table.getValue().getCreate_table_sql();
            if(creatTableSql != null){
                //System.out.println(creatTableSql);
                // 执行CREATE TABLE语句创建表格
                statement.execute(creatTableSql);
            }
        }
        // 关闭连接和Statement对象
        statement.close();
    }

    @Override
    public void flatMap(Message windowMsg, Collector<ChangeLog> out) throws Exception {
        try{
            int subTaskId = getRuntimeContext().getIndexOfThisSubtask();//子任务id/分区编号

            Map<String,List<Message>> resultListMap = windowMsg.getResultListMap();
            if(resultListMap == null){//单条记录时不走reduce
                resultListMap = new HashMap<>();
                List<Message> list = new ArrayList<>();
                list.add(windowMsg);
                resultListMap.put(windowMsg.getDb_table(),list);
            }
            for(Map.Entry<String,List<Message>> r : resultListMap.entrySet()) {
                String db_table =  r.getKey();
                String tableName = db_table.replace(".","_");
                DimensionTable ztable = this.tablesMap.get(tableName);

                List<Message> messageList = r.getValue();

                long start = System.currentTimeMillis();

                if(ztable.getMain_key() != null && ztable.getMain_key().getCol_name() != null){//有主要key
                    for(Message value : messageList){
                        Map<String, String> data = value.getData();
                        String mainKey = data.get(ztable.getMain_key().getCol_name());
                        ChangeLog changeLog = ChangeLog.builder().es(value.getEs()).ts(value.getTs()).ps(value.getPs()).tableName(ztable.getTable_name()).build();
                        changeLog.setMain_key(mainKey);
                        out.collect(changeLog);
                    }
                }else{//无主key
                    List<Map<String, String>> batchList = new ArrayList<>();

                    for(Message value : messageList){
                        Map<String, String> data = value.getData();
                        batchList.add(data);

                        if(batchList.size() != 0 && batchList.size() % BATCH_SIZE == 0 ){
                            ChangeLog changeLog = ChangeLog.builder().es(value.getEs()).ts(value.getTs()).ps(value.getPs()).tableName(ztable.getTable_name()).build();
                            List<String> mainkeys = ztable.findMainKeyBySql(connection, batchList);
                            for (String mainKey : mainkeys) {
                                changeLog.setMain_key(mainKey);
                                out.collect(changeLog);
                            }
                            batchList.clear();
                        }
                    }

                    if(batchList.size() > 0){
                        List<String> mainkeys = ztable.findMainKeyBySql(connection, batchList);
                        for (String mainKey : mainkeys) {
                            Message value = messageList.get(messageList.size()-1);
                            ChangeLog changeLog = ChangeLog.builder().es(value.getEs()).ts(value.getTs()).ps(value.getPs()).tableName(ztable.getTable_name()).build();
                            changeLog.setMain_key(mainKey);
                            out.collect(changeLog);
                        }
                        batchList.clear();
                    }
                }

                Message value = messageList.get(messageList.size()-1);
                long end = System.currentTimeMillis();
                System.out.println("MysqlFlatMap --> subTaskId:" + subTaskId + ",table:" + tableName + ",msg_count:" + messageList.size() + ",usetime:" + (end-start)+",es:" + value.getEs() +",timestamp:"+end);
                log.info("MysqlFlatMap --> subTaskId:" + subTaskId + ",table:" + tableName + ",msg_count:" + messageList.size() + ",usetime:" + (end-start)+",es:" + value.getEs() +",timestamp:"+end);

            }

        }catch (Exception e){
            e.printStackTrace();
            log.error("MessageMysqlFlatMapFunction >> FlatMap Error ",e);
            throw e;
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }
        System.out.println("main key connection close");
    }
}
