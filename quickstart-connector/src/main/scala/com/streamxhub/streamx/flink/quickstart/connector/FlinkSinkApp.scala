/*
 * Copyright (c) 2019 The StreamX Project
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.streamxhub.streamx.flink.quickstart.connector

import com.streamxhub.streamx.flink.connector.kafka.source.KafkaSource
import com.streamxhub.streamx.flink.connector.redis.bean.RedisMapper
import com.streamxhub.streamx.flink.connector.redis.sink.RedisSink
import com.streamxhub.streamx.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand
import org.json4s.DefaultFormats

object FlinkSinkApp extends FlinkStreaming {

  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])
  implicit val userType: TypeInformation[User] = TypeInformation.of(classOf[User])
  implicit val personType: TypeInformation[Person] = TypeInformation.of(classOf[Person])

  override def handle(): Unit = {

    /**
     * 从kafka里读数据.这里的数据是数字或者字母,每次读取1条
     */
    val source = new KafkaSource(context).getDataStream[String]()
      .uid("kfkSource1")
      .name("kfkSource1")
      .map(x => {
        x.value
      })

    //Kafka sink..................
    //2)下沉到目标
    //KafkaSink(context).sink(source)

    val personSource = source.flatMap(x => {
      x.split(",") match {
        case Array(d, a, b, c) => Some(Person(d.toInt, a, b.toInt, c.toInt))
        case _ => None
      }
    })

    val userSource = source.flatMap(x => {
      x.split(",") match {
        case Array(d, a, b, c) => Some(User(d.toInt, a, b.toInt, c.toInt))
        case _ => None
      }
    })

    // Redis sink..................
    //1)定义 RedisSink
    //2)写Mapper映射
    val personMapper: RedisMapper[Person] = RedisMapper.map[Person](
      RedisCommand.HSET,
      "flink_person",
      (x: Person) => x.id.toString,
      (x: Person) => x.name
    )

    RedisSink().sink(personSource, personMapper)

    val userMapper: RedisMapper[User] = RedisMapper.map[User](
      RedisCommand.HSET,
      "flink_user",
      (x: User) => x.id.toString,
      (x: User) => x.name
    )

    RedisSink().sink(userSource, userMapper)

  }

}


case class Person(id: Int, name: String, sex: Int, age: Int)

case class User(id: Int, name: String, sex: Int, age: Int)
