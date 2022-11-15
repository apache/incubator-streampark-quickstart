# streampark-quickstart

Apache StreamPark 开发`Flink`的上手示例程序,分为四个模块:
 - `quickstart-apacheflink`
 - `quickstart-datastream`
 - `quickstart-flinksql`
 - `quickstart-connector`

## quickstart-apacheflink

quickstart-apacheflink 是Flink官方的`SocketWindowWordCount`程序, 其中项目结构,打包规则,开发方式等和Flink官网要求的规范完全一致,
之所以有这个模块,是因为在`streampark-console`中做了对`标准的Flink程序`(按照官方要求的开发规范开发的Flink程序)的部署支持,后续方便演示使用.

## quickstart-datastream

该模块主要演示了如果利用`Apache StreamPark`快速开发一个`DataStream`程序,其中有`java`和`scala`两种语言的开发示例,供开发者快速上手学习使用

## quickstart-flinksql

该模块主要演示了如果利用`Apache StreamPark`快速开发一个`Flink & SQL`程序,其中有`java`和`scala`两种语言的开发示例,供开发者快速上手学习使用

## quickstart-connector

该模块演示了如何使用`Apache StreamPark`里提供的各种 `Datastream connector` , 里面集合了各种 `Datastream connector` 的配置和使用示例, 供开发者快速上手学习使用
