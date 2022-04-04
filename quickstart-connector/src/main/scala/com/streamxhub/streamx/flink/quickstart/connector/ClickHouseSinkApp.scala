package com.streamxhub.streamx.flink.quickstart.connector

import com.streamxhub.streamx.flink.connector.clickhouse.sink.ClickHouseSink
import com.streamxhub.streamx.flink.core.scala.FlinkStreaming
import com.streamxhub.streamx.flink.quickstart.connector.bean.Entity
import org.apache.flink.api.common.typeinfo.TypeInformation

object ClickHouseSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])

  override def handle(): Unit = {
    val createTable =
      """
        |create TABLE test.orders(
        |userId UInt16,
        |orderId UInt16,
        |siteId UInt8,
        |cityId UInt8,
        |orderStatus UInt8,
        |price Float64,
        |quantity UInt8,
        |timestamp UInt16
        |)ENGINE = TinyLog;
        |""".stripMargin

    println(createTable)

    val source = context.addSource(new MyDataSource)

    // 异步写入
    ClickHouseSink().sink[Entity](source)(x => {
      s"(${x.userId},${x.siteId})"
    }).setParallelism(1)

    // 异步写入
    //    ClickHouseSink().sink[TestEntity](source).setParallelism(1)
    // jdbc同步写入写入
    //        ClickHouseSink().syncSink[TestEntity](source)(x => {
    //          s"(${x.userId},${x.siteId})"
    //        }).setParallelism(1)

    // jdbc同步全字段写入
    //    ClickHouseSink().syncSink[TestEntity](source).setParallelism(1)
  }

}
