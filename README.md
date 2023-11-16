<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

<h1 align="center">
   <img src="https://streampark.apache.org/image/logo_name.png" 
   alt="StreamPark Logo" title="Apache StreamPark Logo" width="600"/>
  <br>
</h1>

<h3 align="center">A magical framework that makes stream processing easier!</h3>

<div align="center">

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg?style=for-the-badge&label=license)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![stars](https://img.shields.io/github/stars/apache/streampark?style=for-the-badge&label=stars)](https://github.com/apache/incubator-streampark/stargazers)
[![Latest release](https://img.shields.io/github/v/release/apache/streampark.svg?style=for-the-badge&label=release)](https://github.com/apache/incubator-streampark/releases)
[![total downloads](https://img.shields.io/github/downloads/apache/streampark/total.svg?style=for-the-badge&label=downloads)](https://streampark.apache.org/download)
[![Twitter](https://img.shields.io/twitter/follow/ASFStreamPark?label=follow&logo=twitter&style=for-the-badge)](https://twitter.com/ASFStreamPark)

**[Website](https://streampark.apache.org)**&nbsp;&nbsp;|&nbsp;&nbsp;
**[Document](https://streampark.apache.org/docs/intro)**&nbsp;&nbsp;|&nbsp;&nbsp;
**[FAQ](https://github.com/apache/incubator-streampark/issues/507)**

</div>

## 🚀 What is StreamPark?

<h4>StreamPark is a stream processing development framework and professional management platform. </h4>

> StreamPark is a streaming application development framework. Aimed at ease building and managing streaming applications, StreamPark provides development framework for writing stream processing application with Apache Flink and Apache Spark, More other engines will be supported in the future. Also, StreamPark is a professional management platform for streaming application
, including application development, debugging, interactive query, deployment, operation, maintenance, etc. It was initially known as StreamX and renamed to StreamPark in August 2022.

## 🚀 What is streampark-quickstart ?

> Apache StreamPark quickstart program for developing `Flink`

## 🎉 Modules

* quickstart-apacheflink
* quickstart-datastream
* quickstart-flinksql
* quickstart-connector

### | quickstart-apacheflink
> quickstart-apacheflink is an official SocketWindowWordCount program provided by Flink. The project structure, packaging rules, development methods, etc. are fully consistent with the specifications required by the Flink official website. The reason for having this module is to support the deployment of standard Flink programs (Flink programs developed according to the official development specifications) in streampark-console for subsequent demonstrations and ease of use.

### | quickstart-datastream
> This module demonstrates how to quickly develop a DataStream program using Apache StreamPark. It provides development examples in both Java and Scala to help developers get started quickly.

### | quickstart-flinksql
> This module demonstrates how to quickly develop a Flink & SQL program using Apache StreamPark. It provides development examples in both Java and Scala to help developers get started quickly.

### | quickstart-connector
> This module demonstrates how to use various Datastream connectors provided by Apache StreamPark. It includes configuration and usage examples of various Datastream connectors, allowing developers to get started quickly.

## 🔨 How to Build

```shell
git clone git@github.com:apache/incubator-streampark-quickstart.git
cd incubator-streampark-quickstart
mvn clean install -DskipTests
```

## 💬 Social Media

- [Twitter](https://twitter.com/ASFStreamPark)
- [Zhihu](https://www.zhihu.com/people/streampark) (in Chinese)
- [bilibili](https://space.bilibili.com/455330087) (in Chinese)
- WeChat Official Account (in Chinese, scan the QR code to follow)

<img src="https://streampark.apache.org/image/wx_qr.png" alt="Join the Group" height="350px"><br>
