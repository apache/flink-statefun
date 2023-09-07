# Apache Flink Stateful Functions

Stateful Functions is an API that simplifies the building of **distributed stateful applications** with a **runtime built for serverless architectures**.
It brings together the benefits of stateful stream processing - the processing of large datasets with low latency and bounded resource constraints -
along with a runtime for modeling stateful entities that supports location transparency, concurrency, scaling, and resiliency.

<img alt="Stateful Functions Architecture" width="80%" src="https://github.com/apache/flink-statefun/blob/master/docs/fig/concepts/arch_overview.svg">

It is designed to work with modern architectures, like cloud-native deployments and popular event-driven FaaS platforms
like AWS Lambda and KNative, and to provide out-of-the-box consistent state and messaging while preserving the serverless
experience and elasticity of these platforms.

Stateful Functions is developed under the umbrella of [Apache Flink](https://flink.apache.org/).

This README is meant as a brief walkthrough on the StateFun Python SDK and how to set things up
to get yourself started with Stateful Functions in Python.

For a fully detailed documentation, please visit the [official docs](https://ci.apache.org/projects/flink/flink-statefun-docs-master).

For code examples, please take a look at the [examples](https://github.com/apache/flink-statefun-playground/tree/release-3.3/python).

## Table of Contents

- [Python SDK Overview](#sdkoverview)
- [Contributing](#contributing)
- [License](#license)

## <a name="sdkoverview"></a> Python SDK Overview

### Background

The JVM-based Stateful Functions implementation has a `RequestReply` extension (a protocol and an implementation) that allows calling into any HTTP endpoint that implements that protocol. Although it is possible to implement this protocol independently, this is a minimal library for the Python programing language that:

* Allows users to define and declare their functions in a convenient way.

* Dispatches an invocation request sent from the JVM to the appropriate function previously declared.

### A Mini-Tutorial

#### Define and Declare a Function

```
from statefun import *

functions = StatefulFunctions()

@functions.bind(typename="demo/greeter")
def greet(context, message):
    print(f"Hey {message.as_string()}!")
```

This code declares a function with of type `demo/greeter` and binds it to the instance.

#### Registering and accessing persisted state

You can register persistent state that will be managed by the Stateful Functions workers
for state consistency and fault-tolerance. Values can be generally obtained via the context parameter:

```
from statefun import *

functions = StatefulFunctions()

@functions.bind(
    typename="demo/greeter",
    specs=[ValueSpec(name="seen", type=IntType)])
def greet(context, message):
    seen = context.storage.seen or 0
    seen += 1
    context.storage.seen = seen
    print(f"Hey {message.as_string()} I've seen you {seen} times")
```

#### Expose with a Request Reply Handler

```
handler = RequestReplyHandler(functions)
```

#### Using the Handler with your Favorite HTTP Serving Framework

For example, using Flask:

```
@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler.handle_sync(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response

if __name__ == "__main__":
    app.run()
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

### Testing

1. Create a virtual environment

```
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies

```
pip3 install .
```

3. Run unit tests

```
python3 -m unittest tests
```

## <a name="contributing"></a>Contributing

There are multiple ways to enhance the Stateful Functions API for different types of applications; the runtime and operations will also evolve with the developments in Apache Flink.

You can learn more about how to contribute in the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section and check the _Stateful Functions_ component in [Jira](https://issues.apache.org/jira/browse/FLINK-15969?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20%22Stateful%20Functions%22) for an overview of ongoing community work.

## <a name="license"></a>License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
