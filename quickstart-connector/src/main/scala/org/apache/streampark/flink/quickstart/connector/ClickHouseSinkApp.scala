package org.apache.streampark.flink.quickstart.connector

import org.apache.streampark.flink.connector.clickhouse.sink.ClickHouseSink
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.streampark.flink.quickstart.connector.bean.Entity
import org.apache.flink.api.common.typeinfo.TypeInformation

object ClickHouseSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])

  override def handle(): Unit = {
    // 假如在clickhouse里已经有以下表.
    val createTable =
      """
        |create TABLE test.orders(
        |userId UInt16,
        |siteId UInt8,
        |timestamp UInt16
        |)ENGINE = TinyLog;
        |""".stripMargin

    println(createTable)

    // 1) 接入数据源
    val source = context.addSource(new MyDataSource)

    // 2)高性能异步写入
    ClickHouseSink().asyncSink(source)(x => {s"insert into test.orders(userId,siteId) values (${x.userId},${x.siteId})"})

    //3) jdbc方式写入
    // ClickHouseSink().jdbcSink(source)(x => {s"insert into test.orders(userId,siteId) values (${x.userId},${x.siteId})"})


  }

}
