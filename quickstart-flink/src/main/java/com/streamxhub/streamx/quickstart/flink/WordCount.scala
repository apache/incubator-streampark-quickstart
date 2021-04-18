package com.streamxhub.streamx.quickstart.flink

import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WordCount {

  def main(args: Array[String]): Unit = {
    // the host and the port to connect to
    val hostname = "localhost"
    val port = 9999

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // get input data by connecting to the socket
    val text = env.socketTextStream(hostname, port, "\n")

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text.flatMap(new FlatMapFunction[String,(String,Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        for (word <- value.split("\\s")) {
          out.collect(word,1L)
        }
      }
    }).keyBy(0).timeWindow(Time.seconds(5)).reduce(new ReduceFunction[(String,Long)]() {
      override def reduce(a: (String, Long), b: (String, Long)): (String, Long) = (a._1,a._2 + b._2)
    })

    // print the results with a single thread, rather than in parallel
    windowCounts.print.setParallelism(1)
    env.execute("Socket Window WordCount StreamX NoteBook")
  }
}
