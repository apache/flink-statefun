<img alt="Stateful Functions" src="docs/fig/stateful_functions_logo.png" width=350px/>

Stateful Functions（简称 StateFun）是一个 [Apache Flink](https://flink.apache.org/) 库， __可简化构建分布式有状态应用程序的过程__ 。它基于可持久化状态的函数，这些函数可以在强一致性保证下进行动态交互。

Stateful Functions 使我们能够将强大的状态管理与像 AWS Lambda 类似的 FaaS 实现和 Kubernetes 等现代资源编排框架的弹性、快速扩缩容和滚动升级功能相结合。通过这些特性，它解决了当今许多 FaaS 解决方案中 [最常被引用的两个缺点](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2019/EECS-2019-3.pdf) ：函数间的状态一致性和高效消息传递。

本文档旨在简要介绍 Stateful Functions 的核心概念以及如何开发一个 Stateful Functions 应用。

更多详细信息，可以参考 [官方文档](https://ci.apache.org/projects/flink/flink-statefun-docs-master)。

[![构建状态](https://travis-ci.org/apache/flink-statefun.svg?branch=master)](https://travis-ci.org/apache/flink-statefun)

## 目录

- [核心概念](#core-concepts)
   * [摘要](#abstraction)
   * [函数模块和可扩展性](#modules)
   * [Runtime](#runtime)
- [入门](#getting-started)
   * [运行一个完整的例子](#greeter)
   * [创建项目](#project-setup)
   * [构建项目](#build)
   * [在 IDE 中运行](#ide-harness)
- [应用部署](#deploying)
   * [使用 Docker 映像进行部署](#docker)
   * [作为 Flink 作业部署](#flink)
- [参与贡献](#contributing)
- [开源软件许可](#license)

## <a name="core-concepts"></a>核心概念

### <a name="abstraction"></a>摘要

Stateful Functions 应用程序由以下原语组成：有状态函数，入口（Ingresses），路由（Routers）和出口（Egresses）。

<p align="center">
  <img src="docs/fig/stateful_functions_overview.png" width="650px"/>
</p>

#### Stateful Functions

* 一个 _stateful function_ 是通过消息调用的一小段逻辑/代码。每个 stateful function 都是作为 _函数类型_ 的唯一可
调用 _虚拟实例_ 存在。每个实例都通过其 ``type`` 以及 type 中的唯一 ``ID``（字符串）来寻址。

* Stateful Functions 可以从入口（Ingress）或任何其他的 stateful function（包括其自身）中调用，调用者只需要知道目标函数的逻辑地址即可。

* 函数实例是 _虚拟的_ ，因为它们不总是同时在内存中活跃。在任何一个时间点，只有一小部分函数及其状态作为实际对象存在。
当一个虚拟实例接收到消息时，将配置一个对象并带着该虚拟实例的状态加载，然后处理该消息。与虚拟内存类似，许多函数的状态可能
在任何时间点都被“交换出去”（swap out）。

* 函数的每个虚拟实例都有其自己的状态，可以通过局部变量访问，
并且该状态是私有的，对于该实例来说是本地的。

如果您知道 Apache Flink 的 DataStream API，则可以将 Stateful Functions 考虑为轻量级的 `KeyedProcessFunction` 。函数 ``类型`` 等同于处理函数转换（process function transformation），而 `` ID `` 则是键值（key）。不同之处在于，函数不是在定义数据流（拓扑）的有向无环图（DAG）中组装，而是使用地址将事件任意发送到所有其他函数。

#### 入口和出口

* _入口_ （Ingress）是事件最初到达 Stateful Functions 应用程序的方式。
入口可以是消息队列，日志或 HTTP 服务器 —— 任何可以产生事件并交由应用程序处理的系统。

* _路由_（Router）将入口（Ingress）与 stateful function 连接起来，以确定哪个函数实例应该在最开始时处理来自入口的事件。

* _出口_（Egress）是一种以标准化方式从应用程序发送事件的方法。
出口是可选的，也有可能没有事件需要从应用程序中发送出去，函数会完成事件的处理或直接调用其他应用程序。

### <a name="modules"></a>模块（Module）

_模块_（Module）是将核心构建单元添加到一个 Stateful Functions 应用程序的入口，这些核心构建单元包括：入口（Ingress）、出口（Egress）、路由（Router）和有状态函数。

单个应用程序可以是多个模块（Module）的组合，每个模块都构成了整个应用程序的一部分。
这允许一个 Stateful Functions 应用程序的不同部分由不同的模块来组成，例如：
一个模块可以提供入口和出口，而其他模块可以通过状态函数来独立提供业务逻辑的不同部分。这有助于多个独立团队共同完成较大（复杂）的应用程序。

## <a name="runtime">Runtime

Stateful Functions Runtime 旨在提供一组类似于 [无服务器函数](https://martinfowler.com/articles/serverless.html) 的属性，但适用于有状态的场景。

<p align="center">
  <img src="docs/fig/stateful_functions_overview-ops.png" width="600px"/>
</p>

Runtime 基于 Apache Flink<sup>®</sup> 构建，并具有以下设计原则：

* __逻辑上计算/状态共置__：消息传递，状态访问/更新和函数调用在一起紧密管理，这在抽象层面就天然地保证了一致性。

* __物理上计算/状态分离__：可以远程执行函数计算，并将消息和状态信息作为调用请求的一部分。这样的话，函数（Function）就可以像无状态进程一样管理，并且支持快速扩展、滚动升级和其他常见的运维模式。

* __语言无关性__：函数调用使用一个简单的基于 HTTP/gRPC 的协议，因此可以用各种语言轻松地实现函数。

这使得在 Kubernetes 平台、FaaS 平台上或（微）服务后台运行函数时，在函数之间提供一致的状态保证和轻量级消息传递成为可能。

## <a name="getting-started"></a>入门

按照下面的步骤即可立刻开始使用 Stateful Functions。

本指南将引导您通过设置开始开发和测试自己的 Stateful Functions（Java）应用程序，并运行一个示例。如果您想使用 Python 快速开始，
请查看 [StateFun Python SDK](https://github.com/apache/flink-statefun/tree/master/statefun-sdk-python) 和 [Python Greeter 示例](https://github.com/apache/flink-statefun/tree/master/statefun-examples/statefun-python-greeter-example) 。

### <a name="project-setup"></a>创建项目

前提条件：

* Docker

* Maven 3.5.x 及以上

* Java 8 及以上

您可以使用下面的 Maven 命令快速开始构建 Stateful Functions 应用程序：

```
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=statefun-quickstart \
  -DarchetypeVersion=2.2-SNAPSHOT
```

这使您可以命名新创建的项目。它将以交互方式询问您 `` GroupId ``、
`` ArtifactId ``和 package 名称，并将生成一个与您指定的`` ArtifactId ``同名的新目录。

我们建议您将此项目导入到 IDE 中进行开发和测试。
IntelliJ IDEA 天然支持 Maven 项目。如果使用 Eclipse，则需要使用`` m2e ``插件导入
Maven 项目。一些 Eclipse 发布版本默认包含该插件，而另一些则需要您手动安装。

### <a name="build"></a>构建项目

如果要构建/打包项目，请进入项目目录并运行`` mvn clean package ``命令。您将找到一个包含您的应用程序以及相关依赖的 JAR 包：`target/<artifact-id>-<version>.jar`。

### <a name="ide-harness"></a>在 IDE 中运行

可以直接在 IDE 中运行测试你的程序，无需进一步打包或部署。

请参阅 [Harness 示例](statefun-examples/statefun-flink-harness-example) ，了解如何执行此操作。

### <a name="greeter"></a>运行一个完整的例子

作为一个简单的演示，我们将逐步运行 [Greeter 示例](statefun-examples/statefun-greeter-example)。

在进行其他操作之前，请确保已在本地 [构建项目以及基本的 Stateful Functions Docker 映像](#build) 。
然后，按照以下步骤运行示例：

```
cd statefun-examples/statefun-greeter-example
docker-compose build
docker-compose up
```

该示例包含一个非常基本的 Stateful Functions，包含 Kafka 入口和 Kafka 出口。

要查看实际的运行情况，需要向 topic `` names `` 发送一些消息，并查看 topic `` greetings `` 的输出：

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

## <a name="deploying"></a>应用部署

Stateful Functions 应用程序可以打包为一个 [独立应用程序](https://ci.apache.org/projects/flink/flink-statefun-docs-master/deployment-and-operations/packaging.html#images) 或者作为一个 [Flink 作业](https://ci.apache.org/projects/flink/flink-statefun-docs-master/deployment-and-operations/packaging.html#flink-jar) 提交给 Flink 集群运行。

### <a name="docker"></a>使用 Docker 映像进行部署

以下是一个 Dockerfile 示例，用于为名为`` statefun-example ``的应用程序构建带有 [嵌入式](https://ci.apache.org/projects/flink/flink-statefun-docs-master/sdk/modules.html#embedded-module) 模块（Java）的 Stateful Functions 镜像。

```
FROM flink-statefun[:version-tag]

RUN mkdir -p /opt/statefun/modules/statefun-example

COPY target/statefun-example*jar /opt/statefun/modules/statefun-example/
```

### <a name="flink"></a>作为 Flink 作业部署

如果您希望将 Stateful Functions 应用程序打包为 Flink 作业以提交到现有的 Flink 集群，只需在你的应用程序中将`` statefun-flink-distribution ``添加为依赖项。

```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>statefun-flink-distribution</artifactId>
    <version>2.2-SNAPSHOT</version>
</dependency>
```

它包括所有运行时依赖项，并配置应用程序的主入口点。
除了将依赖项添加到 POM 文件之外，您无需执行任何其他操作。

<strong>注意:</strong>该发行版必须捆绑在你的应用程序 fat JAR 中，以将它放置于 Flink 的 [用户代码类加载器上](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/debugging_classloading.html#inverted-class-loading-and-classloader-resolution-order)

  ```
{$FLINK_DIR}/bin/flink run ./statefun-example.jar
```

## <a name="contributing"></a>参与贡献

有多种方法可以为不同类型的应用程序增强 Stateful Functions API。Runtime 和运维也将随着 Apache Flink 的发展而发展。

您可以在 [Apache Flink 网站](https://flink.apache.org/contributing/how-to-contribute.html) 上了解关于如何做出贡献的更多信息。对于代码贡献，请仔细阅读“ [贡献代码”](https://flink.apache.org/contributing/contribute-code.html) 部分，并检查 [Jira](https://issues.apache.org/jira/browse/FLINK-15969?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20%22Stateful%20Functions%22) 中的 _Stateful Functions_ 组件以概要了解正在进行中的社区工作。

## <a name="license"></a>开源软件许可

该仓库中的代码根据 [Apache Software License 2](LICENSE) 开源。