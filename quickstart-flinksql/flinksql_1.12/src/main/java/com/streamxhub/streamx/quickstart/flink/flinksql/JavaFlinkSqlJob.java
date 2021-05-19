package com.streamxhub.streamx.quickstart.flink.flinksql;


import com.streamxhub.streamx.flink.core.TableContext;
import com.streamxhub.streamx.flink.core.TableEnvConfig;

public class JavaFlinkSqlJob {

    public static void main(String[] args) {
        TableEnvConfig tableEnvConfig = new TableEnvConfig(args, (tableConfig, parameterTool) -> {
            System.out.println("set tableConfig...");
        });

        TableContext context = new TableContext(tableEnvConfig);
        context.sql("flinksql");
    }

}
