package com.streamxhub.streamx.flink.quickstart.connector

import com.streamxhub.streamx.flink.connector.kafka.source.KafkaSource
import com.streamxhub.streamx.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object KafkaSourceApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[String] = TypeInformation.of(classOf[String])


  /**
   * 用户可覆盖次方法...
   *
   */
  override def ready(): Unit = super.ready()


  override def config(env: StreamExecutionEnvironment,
                      parameter: ParameterTool): Unit = {

  }

  override def handle(): Unit = {

    //one topic
    KafkaSource().getDataStream[String]()
      .uid("kfkSource1")
      .name("kfkSource1")
      .map(x => {
        x.value
      })
      .print()

  }

}
