# Apache Flink Stateful Functions

Stateful Functions is an API that simplifies the building of **distributed stateful applications** with a **runtime built for serverless architectures**.
It brings together the benefits of stateful stream processing - the processing of large datasets with low latency and bounded resource constraints -
along with a runtime for modeling stateful entities that supports location transparency, concurrency, scaling, and resiliency.

<img alt="Stateful Functions Architecture" width="80%" src="https://github.com/apache/flink-statefun/blob/master/docs/fig/concepts/arch_overview.svg">

It is designed to work with modern architectures, like cloud-native deployments and popular event-driven FaaS platforms
like AWS Lambda and KNative, and to provide out-of-the-box consistent state and messaging while preserving the serverless
experience and elasticity of these platforms.

Stateful Functions is developed under the umbrella of [Apache Flink](https://flink.apache.org/).

This README is meant as a brief walkthrough on the StateFun JavaScript SDK for NodeJS and how to set things up
to get yourself started with Stateful Functions in JavaScript.

For a fully detailed documentation, please visit the [official docs](https://ci.apache.org/projects/flink/flink-statefun-docs-master).

For code examples, please take a look at the [examples](https://github.com/apache/flink-statefun-playground/tree/release-3.3/javascript).

## Table of Contents

- [JavaScript SDK for NodeJS Overview](#sdkoverview)
- [Contributing](#contributing)
- [License](#license)

## <a name="sdkoverview"></a> JavaScript SDK for NodeJS Overview

### Background

The JVM-based Stateful Functions implementation has a `RequestReply` extension (a protocol and an implementation) that allows calling into any HTTP endpoint that implements that protocol. Although it is possible to implement this protocol independently, this is a minimal library for the JavaScript programing language that:

* Allows users to define and declare their functions in a convenient way.

* Dispatches an invocation request sent from the JVM to the appropriate function previously declared.

### A Mini-Tutorial

#### Define and Declare a Function

```
const {Context, Message, StateFun} = require("apache-flink-statefun");

let statefun = new StateFun();
statefun.bind({
	typename: "example/greeter",
	fn(context, message) {
    console.log("Hey %s!", message.asString())
  }
});
```

This code declares a function with of type `example/greeter` and binds it to the instance.

#### Registering and accessing persisted state

You can register persistent state that will be managed by the Stateful Functions workers
for state consistency and fault-tolerance. Values can be generally obtained via the context parameter:

```
const {Context, Message, StateFun} = require("apache-flink-statefun");

let statefun = new StateFun();
statefun.bind({
    typename: "example/greeter",
    fn(context, message) {
        const name = message.asString();
        let seen = context.storage.seen || 0;
        seen = seen + 1;
        context.storage.seen = seen;

        console.log("Hello %s for the %dth time!", name, seen);
    },
    specs: [{
        name: "seen",
        type: StateFun.intType(),
    }]
});
```

#### Exposing the Request Reply Handler

```
const http = require("http");
const {Context, Message, StateFun} = require("apache-flink-statefun");

let statefun = new StateFun();
//...

http.createServer(statefun.handler()).listen(8000);
```

This creates an HTTP server that accepts requests from the Stateful Functions cluster and
dispatches it to the handler.

#### Composing the Module YAML File

The remaining step would be to declare this function type in a module.yaml

```
functions:
  - function:
    meta:
      kind: http
      type: demo/greeter
    spec:
      endpoint: http://<end point url>/statefun
```

## <a name="contributing"></a>Contributing

There are multiple ways to enhance the Stateful Functions API for different types of applications; the runtime and operations will also evolve with the developments in Apache Flink.

You can learn more about how to contribute in the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section and check the _Stateful Functions_ component in [Jira](https://issues.apache.org/jira/browse/FLINK-15969?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20%22Stateful%20Functions%22) for an overview of ongoing community work.

## <a name="license"></a>License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
