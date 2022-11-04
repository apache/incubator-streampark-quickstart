package org.apache.streampark.flink.quickstart.connector

import org.apache.flink.streaming.api.scala._
import org.apache.streampark.flink.connector.kafka.source.KafkaSource
import org.apache.streampark.flink.core.scala.FlinkStreaming

object KafkaSourceApp extends FlinkStreaming {
  override def handle(): Unit = {

    KafkaSource().getDataStream[String]()
      .map(_.value)
      .print()

  }

}
