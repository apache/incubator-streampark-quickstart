package com.streamxhub.streamx.quickstart.flink.flinksql;

import com.streamxhub.streamx.flink.core.scala.TableContext;
import com.streamxhub.streamx.flink.core.scala.util.TableEnvConfig;

public class JavaFlinkSqlJob {

    public static void main(String[] args) {
        TableEnvConfig tableEnvConfig = new TableEnvConfig(args, (tableConfig, parameterTool) -> {
            System.out.println("set tableConfig...");
        });

        TableContext context = new TableContext(tableEnvConfig);
        context.sql("flinksql");
    }

}
