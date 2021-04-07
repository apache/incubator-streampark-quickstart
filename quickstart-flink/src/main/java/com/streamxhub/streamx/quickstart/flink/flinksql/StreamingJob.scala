package com.streamxhub.streamx.quickstart.flink.flinksql

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

/**
 * 整个项目工程是完全按照flink官方的标准
 */
object StreamingJob extends App {

  private val KAFKA_BROKER = "dp-kafka-1:9092,dp-kafka-2:9092,dp-kafka-3:9092,dp-kafka-4:9092,dp-kafka-5:9092"
  private val TRANSACTION_GROUP = "streamx_test_001"

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.enableCheckpointing(1000)
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

  // configure Kafka consumer
  val kafkaProps = new Properties()
  kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
  kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
  kafkaProps.setProperty("auto.offset.reset", "earliest")

  val source = new FlinkKafkaConsumer[String]("hopsonone_park_order_stat", new SimpleStringSchema(), kafkaProps)
  env.addSource(source).print()

  env.execute()


}
