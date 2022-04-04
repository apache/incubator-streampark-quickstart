package com.streamxhub.streamx.flink.quickstart.connector

import com.streamxhub.streamx.flink.connector.redis.bean.RedisMapper
import com.streamxhub.streamx.flink.connector.redis.sink.RedisSink
import com.streamxhub.streamx.flink.core.scala.FlinkStreaming
import com.streamxhub.streamx.flink.quickstart.connector.bean.Entity
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand

object FlinkRedisSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])

  override def handle(): Unit = {

    /**
     * 创造读取数据的源头
     */
    val source = context.addSource(new MyDataSource)

    // Redis sink..................
    //1)定义 RedisSink
    val sink = RedisSink()
    //2)写Mapper映射
    val personMapper: RedisMapper[Entity] = RedisMapper.map(RedisCommand.HSET, "flink_user", (x: Entity) => x.userId.toString, (x: Entity) => x.userId.toString)
    sink.sink[Entity](source, personMapper, 60000000).setParallelism(1)

  }

}
