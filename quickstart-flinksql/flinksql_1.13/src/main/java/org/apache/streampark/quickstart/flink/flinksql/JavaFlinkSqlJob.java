package org.apache.streampark.quickstart.flink.flinksql;


import org.apache.streampark.flink.core.TableContext;
import org.apache.streampark.flink.core.TableEnvConfig;

public class JavaFlinkSqlJob {

    public static void main(String[] args) {
        TableEnvConfig tableEnvConfig = new TableEnvConfig(args, (tableConfig, parameterTool) -> {
            System.out.println("set tableConfig...");
        });

        TableContext context = new TableContext(tableEnvConfig);
        context.sql("flinksql");
    }

}
