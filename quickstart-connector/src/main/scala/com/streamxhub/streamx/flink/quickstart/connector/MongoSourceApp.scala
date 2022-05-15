package com.streamxhub.streamx.flink.quickstart.connector

import com.mongodb.BasicDBObject
import com.streamxhub.streamx.common.util.{DateUtils, JsonUtils}
import com.streamxhub.streamx.flink.connector.mongo.source.MongoSource
import com.streamxhub.streamx.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties
import scala.collection.JavaConversions._

object MongoSourceApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[String] = TypeInformation.of(classOf[String])

  override def handle(): Unit = {
    implicit val prop: Properties = context.parameter.getProperties
    val source = MongoSource()
    source.getDataStream[String](
      "shop",
      (a, d) => {
        Thread.sleep(1000)
        /**
         * 从上一条记录提前offset数据,作为下一条数据查询的条件,如果offset为Null,则表明是第一次查询,需要指定默认offset
         */
        val offset = if (a == null) "2019-09-27 00:00:00" else {
          JsonUtils.read[Map[String, _]](a).get("updateTime").toString
        }
        val cond = new BasicDBObject().append("updateTime", new BasicDBObject("$gte", DateUtils.parse(offset)))
        d.find(cond)
      },
      _.toList.map(_.toJson()), null
    ).print()
  }

}
