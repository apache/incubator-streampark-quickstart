/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.streampark.flink.quickstart.datastream

import org.apache.streampark.common.util.JsonUtils
import org.apache.streampark.flink.connector.jdbc.sink.JdbcSink
import org.apache.streampark.flink.connector.kafka.source.KafkaSource
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation

object QuickStartApp extends FlinkStreaming {

  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])
  implicit val userType: TypeInformation[User] = TypeInformation.of(classOf[User])

  override def handle(): Unit = {
    val source = KafkaSource()
      .getDataStream[String]()
      .map(x => JsonUtils.read[User](x.value))
      .filter(_.age < 30)

    JdbcSink().sink[User](source)(user =>
      s"""
         |insert into t_user(`name`,`age`,`gender`,`address`)
         |value('${user.name}',${user.age},${user.gender},'${user.address}')
         |""".stripMargin
    )
  }

}

case class User(name: String, age: Int, gender: Int, address: String)

