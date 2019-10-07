<img alt="Stateful Functions" src="stateful-functions-docs/images/stateful_functions_logo.png" width=400px/>

Stateful Functions is a library for distributed applications and services, based on, well, you guessed it: stateful functions.

The project aims to simplify the development of distributed stateful applications by solving some of the common
challenges in those applications: scaling, consistent state management, reliable interaction between distributed
services, and resource management.

Stateful Functions uses [Apache Flink](https://flink.apache.org/) for distributed coordination, state, and communication.

This README is meant as a brief walkthrough on the core concepts and how to set things up
to get yourself started with Stateful Functions. For a fully detailed documentation, please
see [https://statefun.io](https://statefun.io).

## Table of Contents

- [Core Concepts](#core-concepts)
   * [Abstraction](#abstraction)
   * [Function modules and extensibility](#modules)
- [Getting Started](#getting-started)
   * [Building the project](#build)
   * [Running a full example](#greeter)
   * [Project setup](#project-setup)
   * [Running in the IDE](#ide-harness)
- [Deploying Applications](#deploying)
   * [Deploying with a Docker image](#docker)
   * [Deploying as a Flink job](#flink)
- [Contributing](#contributing)
- [License](#license)

## <a name="core-concepts"></a>Core Concepts

### <a name="abstraction"></a>Abstraction

A Stateful Functions application consists of the following primitives: stateful functions, ingresses,
routers, and egresses.

#### Stateful functions

* Stateful functions are the building blocks and namesake of the Stateful Functions framework.
A function is a small piece of logic (currently simple Java functions) that are invoked through a message.

* Each stateful function exist as uniquely invokable _virtual instances_ of a _function type_. Each instance
is addressed by its type, as well as an unique id (a string) within its type.

* Stateful functions may be invoked from ingresses or any other stateful function (including itself).
The caller simply needs to know the address of the target function.

* Function instances are _virtual_, because they are not all active in memory at the same time.
At any point in time, only a small set of functions and their state exists as actual objects. When a
virtual instance receives a message, one of the objects is configured and loaded with the state of that virtual
instance and then processes the message. Similar to virtual memory, the state of many functions might be “swapped out”
at any point in time.

* Each virtual instance of a function has its own state, which can be accessed in local variables.
That state is private and local to that instance.

If you know Apache Flink’s DataStream API, you can think of stateful functions a bit like a lightweight
`KeyedProcessFunction`. The function type is process function transformation, while the ID is the key. The difference
is that functions are not assembled in a directed acyclic graph that defines the flow of data (the streaming topology),
but rather send events arbitrarily to all other functions using addresses.

#### Ingresses and Egresses

* _Ingresses_ are the way that events initially arrive in a Stateful Functions application.
Ingresses can be message queues, logs, or HTTP servers - anything that produces an event to be
handled by the application.

* _Routers_ are attached to ingresses to determine which function instance should handle an event initially.

* _Egresses_ are a way to send events out from the application in a standardized way.
Egresses are optional; it is also possible that no events leave the application and functions sink events or
directly make calls to other applications.

### <a name="modules"></a>Modules and extensibility

A _module_ is the entry point for adding to a Stateful Functions
application the core building block primitives, i.e. ingresses, egresses, routers, and stateful functions.

A single application may be a combination of multiple modules, each contributing a part of the whole application.
This allows different parts of the application to be contributed by different modules; for example,
one module may provide ingresses and egresses, while other modules may individually contribute specific parts of the
business logic as stateful functions. This facilitates working in independent teams, but still deploying
into the same larger application.

This extensibility is achieved by leveraging the [Java Service Loader](https://docs.oracle.com/javase/tutorial/ext/basics/spi.html#the-serviceloader-class).
In this context, each module is essentially a service provider.

## <a name="getting-started"></a>Getting Started

Follow the steps here to get started right away with Stateful Functions.

This guide will walk you through locally building the project, running an existing example, and setup to
start developing and testing your own Stateful Functions application.

### <a name="build"></a>Building the project

Prerequisites:

* Docker
* Maven 3.5.x or above
* Java 8 or above

Currently, the project does not have any publicly available artifacts or Docker images for use, so you would have to
first build the project yourself before trying it out.

```
mvn clean install
```

If you want to [deploy your applications using Docker](#docker), you should also build the base Docker image:

```
./tools/docker/build-stateful-functions.sh
```

### <a name="greeter"></a>Running a full example

As a simple demonstration, we will be going through the steps to run the [Greeter example](stateful-functions-examples/stateful-functions-greeter-example).

Before anything else, make sure that you have locally [built the project as well as the base Stateful Functions Docker image](#build).
Then, follow the next steps to run the example:

```
cd stateful-functions-examples/stateful-functions-greeter-example
docker-compose build
docker-compose up
```

This example contains a very basic stateful function with a Kafka ingress and a Kafka egress.

To see the example in action, send some messages to the topic `names`, and see what comes out out of the topic `greetings`:

```
KAFKA=$(docker ps -f "name=stateful-functions-greeter-example_kafka-broker_1" --format "{{.ID}}") ; \
docker exec -it $KAFKA kafka-console-producer.sh \
     --broker-list localhost:9092 \
     --topic names
```

```
KAFKA=$(docker ps -f "name=stateful-functions-greeter-example_kafka-broker_1" --format "{{.ID}}") ; \
docker exec -it $KAFKA kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic greetings
```

### <a name="project-setup"></a>Project setup

You can quickly get started building Stateful Functions applications using the provided quickstart Maven archetype:

```
mvn archetype:generate \
  -DarchetypeGroupId=com.ververica \
  -DarchetypeArtifactId=stateful-functions-quickstart \
  -DarchetypeVersion=1.0-SNAPSHOT
```

This allows you to name your newly created project. It will interactively ask you for the groupId,
artifactId, and package name. There will be a new directory with the same name as your artifact id.

We recommend you import this project into your IDE to develop and test it.
IntelliJ IDEA supports Maven projects out of the box. If you use Eclipse, the `m2e` plugin allows to import
Maven projects. Some Eclipse bundles include that plugin by default, others require you to install it manually.

### <a name="ide-harness"></a>Running from the IDE

To test out your application, you can directly run it in the IDE without any further packaging or deployments.

Please see the [Harness example](stateful-functions-examples/stateful-functions-flink-harness-example) on how to do that.

## <a name="deploying"></a>Deploying Applications

Stateful Functions applications can be packaged as either standalone applications or Flink jobs that can be
submitted to a Flink cluster.

### <a name="docker"></a>Deploying with a Docker image

Below is an example Dockerfile for building an image for an application called `stateful-functions-example`:

```
FROM stateful-functions

RUN mkdir -p /opt/stateful-functions/modules/stateful-functions-example
COPY target/stateful-functions-example*jar /opt/stateful-functions/modules/stateful-functions-example/
```

### <a name="flink"></a>Deploying as a Flink job

If you prefer to package your Stateful Functions application as a Flink job to submit to an existing Flink cluster,
simply include `stateful-functions-flink-distribution` as a dependency to your application.

```
<dependency>
    <groupId>com.ververica</groupId>
    <artifactId>stateful-functions-flink-distribution</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

It includes all the runtime dependencies and configures the application's main entry-point.
You do not need to take any action beyond adding the dependency to your Maven pom.

Bundle the distribution with your application as a fat jar, and then submit it as you normally would
with any other Flink job:

```
{$FLINK_DIR}/bin/flink run ./stateful-functions-example.jar
```

## <a name="contributing"></a>Contributing

If you find these ideas interesting or promising, try Stateful Functions out and get involved!
Check out the example walkthrough or the docs. File an issue if you have an idea how to improve things.

The project is work-in-progress. We believe we are off to a promising direction, but there is still a
way to go to make all parts of this vision a reality. There are many possible ways to enhance the Stateful
Functions API for different types of applications. Runtime and operations of Stateful Functions
will also evolve with the capabilities of Apache Flink.

## <a name="license"></a>License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
