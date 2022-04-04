package com.streamxhub.streamx.flink.quickstart.connector

import com.streamxhub.streamx.flink.connector.http.sink.HttpSink
import com.streamxhub.streamx.flink.core.scala.FlinkStreaming
import com.streamxhub.streamx.flink.quickstart.connector.bean.Entity
import org.apache.flink.api.common.typeinfo.TypeInformation

object FlinkHttpSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])
  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])

  override def handle(): Unit = {

    /**
     * source
     */
    val source = context.addSource(new MyDataSource)
      .map(x => s"http://www.qq.com?id=${x.userId}")

    // sink
    new HttpSink(context).get(source)

  }

}
