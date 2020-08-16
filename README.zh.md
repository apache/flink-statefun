<img alt="有状态函数" src="docs/fig/stateful_functions_logo.png" width=350px/>

有状态函数是一个[Apache Flink](https://flink.apache.org/)库， __可简化构建分布式有状态应用程序的过程__ 。它基于有着可持久化状态的函数，这些函数可以在强大的一致性保证下进行动态交互。 

有状态函数使强大的状态管理和组合，与AWS Lambda之类的FaaS实现和Kubernetes之类的现代资源编排框架的弹性，快速缩放/零缩放和滚动升级功能相结合成为可能。通过这些特性，它解决了当今许多FaaS设置中[最常被引用的两个缺点](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2019/EECS-2019-3.pdf) ：状态一致和函数之间的高效消息传递。 

本自述文件旨在简要介绍核心概念以及如何进行设置
以使您开始使用有状态函数。 

有关详细文档，请访问[官方文档](https://ci.apache.org/projects/flink/flink-statefun-docs-master) 。 

有关代码示例，请查看[示例](statefun-examples/) 。 

 [![构建状态](https://travis-ci.org/apache/flink-statefun.svg?branch=master)](https://travis-ci.org/apache/flink-statefun) 

## 目录

- [核心概念](#core-concepts) 
   * [抽象化](#abstraction) 
   * [函数模块和可扩展性](#modules) 
   * [运行时](#runtime) 
- [入门](#getting-started) 
   * [运行一个完整的例子](#greeter) 
   * [新建项目](#project-setup) 
   * [构建项目](#build) 
   * [在IDE Harness中运行](#ide-harness) 
- [部署应用](#deploying) 
   * [使用Docker映像进行部署](#docker) 
   * [部署为Flink作业](#flink) 
- [贡献](#contributing) 
- [开源软件许可](#license) 

## <a name="core-concepts"></a>核心概念

### <a name="abstraction"></a>抽象化

有状态函数应用程序由以下原语组成：有状态函数，入口(ingress)，
路由器(router)和出口(egress)。 

<p align="center">
  <img src="docs/fig/stateful_functions_overview.png" width="650px"/>
</p>

#### 有状态函数

* _有状态函数_是通过消息调用的一小段逻辑/代码。每个有状态函数
都作为_函数类型_的唯一可调用_虚拟实例_存在。每个实例都通过其``type``以及type中的唯一``ID`` （字符串）来寻址。 

* 有状态函数可以从ingress或任何其他有状态函数（包括其自身）中调用。
调用者只需要知道目标函数的地址即可。 

* 函数实例是_虚拟的_ ，因为它们不总是同时在内存中活跃。
在任何时间点，只有一小部分函数及其状态作为实际对象存在。当
虚拟实例接收到消息时，将配置一个对象并带着该虚拟实例的状态
加载，然后处理该消息。与虚拟内存类似，许多函数的状态可能
在任何时间点都被“交换出去”(swap out)。 

* 函数的每个虚拟实例都有其自己的状态，可以通过局部变量访问。
该状态是私有的，对于该实例是本地的(local)。

如果您知道Apache Flink的DataStream API，则可以将有状态函数考虑为轻量级的
`KeyedProcessFunction` 。函数``type``等同于处理函数转换(process function transformation)，而`` ID ``是键(key)。不同之处
在于，函数不是在定义数据流的有向非循环图（DAG）中组装（流拓扑），
而是使用地址将事件任意发送到所有其他函数。 

#### 入口和出口

* _入口_ (Ingress)是事件最初到达有状态函数应用程序的方式。
入口可以是消息队列，日志或HTTP服务器-产生事件并由
应用程序处理的任何物件。 

* _路由器_(Router)连接到入口(Ingress)，以确定哪个函数实例应最初处理事件。 

* _出口_(Egress)是一种以标准化方式从应用程序发送事件的方法。
出口是可选的；也有可能没有事件离开应用程序和函数接收器(functions sink)事件，或
直接调用其他应用程序。 

### <a name="modules"></a>模块(Module)

 _模块_(Module)将核心构建基元添加到有状态函数
 应用程序的入口点，即入口，出口，路由器和有状态函数。 

单个应用程序可以是多个模块的组合，每个模块都构成整个应用程序的一部分。
这允许应用程序的不同部分由不同的模块来组成。例如，
一个模块可以提供入口和出口，而其他模块可以分别作为状态函数来
贡献业务逻辑的特定部分。这有助于在独立团队中工作，但仍可以部署到
相同的更大应用程序中。 

## <a name="runtime">运行时 

有状态函数运行时旨在提供一组类似于[无服务器函数](https://martinfowler.com/articles/serverless.html)属性的属性，但适用于有状态问题。 

<p align="center">
  <img src="docs/fig/stateful_functions_overview-ops.png" width="600px"/>
</p>

运行时建立在Apache Flink<sup>®</sup>，并具有以下设计原则：

*  __逻辑计算/状态共置：__消息传递，状态访问/更新和函数调用在一起紧密管理。这确保了开箱即用地在高层次支持一致性。 <!--TODO: disambiguate high-level 更抽象的(eg. high-level API) vs. 更高度的(eg. a high level of activity) -->

* __物理计算/状态分离：__可以远程执行函数，并将消息和状态访问作为调用请求的一部分提供。这样，可以像无状态进程一样管理函数，并支持快速扩展，滚动升级和其他常见的操作模式。 

* __语言独立性：__函数调用使用简单的HTTP协议/基于gRPC的协议，因此可以轻松地以各种语言实现函数。  

这使得可以在Kubernetes部署，在FaaS平台上或（微）服务后面执行函数，同时在函数之间提供一致的状态和轻量级消息传递。 

## <a name="getting-started"></a>入门

请按照此处的步骤立即开始使用有状态函数。 

本指南将引导您进行设置以开始开发和测试自己的状态函数（Java）应用程序，并运行现有示例。如果您想使用Python快速开始，
请查看[StateFun Python SDK](https://github.com/apache/flink-statefun/tree/master/statefun-python-sdk)和[Python Greeter示例](https://github.com/apache/flink-statefun/tree/master/statefun-examples/statefun-python-greeter-example) 。 

### <a name="project-setup"></a>项目设置

前提条件： 

* Docker  
    
* Maven 3.5.x 及以上
    
* Java 8 及以上

您可以使用提供的快速入门Maven原型快速开始构建Stateful Function有状态函数应用程序： 

```
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=statefun-quickstart \
  -DarchetypeVersion=2.2-SNAPSHOT
```

这使您可以命名新创建的项目。它将以交互方式询问您`` GroupId `` ， 
`` ArtifactId ``和程序包名称。将有一个与您的`` ArtifactId ``同名的新目录。 

我们建议您将此项目导入到IDE中进行开发和测试。 
IntelliJ IDEA开箱即用地支持Maven项目。如果使用Eclipse，则`` m2e ``插件允许导入
Maven项目。某些Eclipse捆绑包默认包含该插件，而另一些则需要您手动安装。 

### <a name="build"></a>建设项目

如果要构建/打包项目，请转到项目目录并运行`` mvn clean package ``命令。您将找到一个包含您的应用程序的JAR文件，以及可能已作为依赖关系添加到该应用程序的任何库：`target/<artifact-id>-<version>.jar`。 

### <a name="ide-harness"></a>从IDE Harness运行

要测试您的应用程序，可以直接在IDE中运行它，而无需进行任何进一步的打包或部署。 

请参阅[Harness示例](statefun-examples/statefun-flink-harness-example) ，了解如何执行此操作。 

### <a name="greeter"></a>运行一个完整的例子

作为一个简单的演示，我们将逐步完成运行[Greeter示例](statefun-examples/statefun-greeter-example)的步骤。 

在进行其他操作之前，请确保已在本地[构建项目以及基本的Stateful Functions Docker映像](#build) 。
然后，按照以下步骤运行示例： 

```
cd statefun-examples/statefun-greeter-example
docker-compose build
docker-compose up
```

该示例包含一个非常基本的有状态函数，具有Kafka入口和Kafka出口。 

要查看实际示例，请向topic `` names ``发送一些消息，并查看topic `` greetings `` ： 

```
docker-compose exec kafka-broker kafka-console-producer.sh \
     --broker-list localhost:9092 \
     --topic names
```

```
docker-compose exec kafka-broker kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --isolation-level read_committed \
     --from-beginning \
     --topic greetings 
```

## <a name="deploying"></a>部署应用

状态函数应用程序可以打包为[独立应用程序](https://ci.apache.org/projects/flink/flink-statefun-docs-master/deployment-and-operations/packaging.html#images)或[Flink作业](https://ci.apache.org/projects/flink/flink-statefun-docs-master/deployment-and-operations/packaging.html#flink-jar) ，可以提交给Flink群集。 

### <a name="docker"></a>使用Docker映像进行部署

以下是一个示例Dockerfile，用于为名为`` statefun-example ``的应用程序构建带有[嵌入式](https://ci.apache.org/projects/flink/flink-statefun-docs-master/sdk/modules.html#embedded-module)模块（Java）的有状态函数映像。 

```
FROM flink-statefun[:version-tag]

RUN mkdir -p /opt/statefun/modules/statefun-example

COPY target/statefun-example*jar /opt/statefun/modules/statefun-example/
```

### <a name="flink"></a>部署为Flink作业

如果您希望将Stateful Functions应用程序打包为Flink作业以提交到现有的Flink集群，只需将`` statefun-flink-distribution ``包含为对应用程序的依赖项。 

```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>statefun-flink-distribution</artifactId>
    <version>2.2-SNAPSHOT</version>
</dependency>
```

它包括所有运行时依赖项，并配置应用程序的主入口点。
除了将依赖项添加到POM文件之外，您无需执行任何其他操作。 

<div class="alert alert-info">
  <strong>Attention:</strong>该发行版必须捆绑在您的应用程序胖JAR中，以将它放置于Flink的[用户代码类加载器上](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/debugging_classloading.html#inverted-class-loading-and-classloader-resolution-order) 
  </div>

```
{$FLINK_DIR}/bin/flink run ./statefun-example.jar
```

## <a name="contributing"></a>贡献

有多种方法可以为不同类型的应用程序增强Stateful Functions API。运行时和操作也将随着Apache Flink的发展而发展。 

您可以在[Apache Flink网站上](https://flink.apache.org/contributing/how-to-contribute.html)了解有关如何做出贡献的更多信息。对于代码贡献，请仔细阅读“ [贡献代码”](https://flink.apache.org/contributing/contribute-code.html)部分，并检查[Jira](https://issues.apache.org/jira/browse/FLINK-15969?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20%22Stateful%20Functions%22)中的_Stateful Functions_组件以获取正在进行的社区工作的概述。 

## <a name="license"></a>开源软件许可

该仓库中的代码根据[Apache Software License 2](LICENSE) 开源。 