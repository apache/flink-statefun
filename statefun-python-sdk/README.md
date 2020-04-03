# Apache Flink Stateful Functions

Stateful Functions is an [Apache Flink](https://flink.apache.org/) library that **simplifies building distributed stateful applications**. It's based on functions with persistent state that can interact dynamically with strong consistency guarantees.

Stateful Functions makes it possible to combine a powerful approach to state and composition with the elasticity, rapid scaling/scale-to-zero and rolling upgrade capabilities of FaaS implementations like AWS Lambda and modern resource orchestration frameworks like Kubernetes. With these features, it addresses [two of the most cited shortcomings](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2019/EECS-2019-3.pdf) of many FaaS setups today: consistent state and efficient messaging between functions.

This README is meant as a brief walkthrough on the StateFun Python SDK and how to set things up
to get yourself started with Stateful Functions in Python.

For a fully detailed documentation, please visit the [official docs](https://ci.apache.org/projects/flink/flink-statefun-docs-master).

For code examples, please take a look at the [examples](../statefun-examples/).

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
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("demo/greeter")
def greet(context, message: LoginEvent):
    print("Hey " + message.user_name)
```

This code declares a function with a `FunctionType("demo", "greeter")` and binds the greet Python instance to it.

#### Expose with a Request Reply Handler

```
from statefun import RequestReplyHandler

handler = RequestReplyHandler(functions)
```

#### Using the Handler with your Favorite HTTP Serving Framework

For example, using Flask:

``` 
@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler(request.data)
    esponse = make_response(response_data)
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
      states:
        - foo
        - bar
        - baz
```

#### Eager State Registration

The request reply protocol requires that the state names would be registered in the module YAML file
under the `states` section (see the example above). The state values could be absent (`None` or a `google.protobuf.Any`) and they can be generally obtained via the context parameter:

```
@functions.bind("demo/greeter")
def greet(context, message: LoginEvent):
    session = context['session']
    if not session:
       session = start_session(message)
       context['session'] = session
    ...

```

### Testing

1. Create a virtual environment

```
python3 -m venv venv
source venv/bin/activate
```

2. Run unit tests

```
python3 -m unittest tests
```

## <a name="contributing"></a>Contributing

There are multiple ways to enhance the Stateful Functions API for different types of applications; the runtime and operations will also evolve with the developments in Apache Flink.

You can learn more about how to contribute in the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section and check the _Stateful Functions_ component in [Jira](https://issues.apache.org/jira/browse/FLINK-15969?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20%22Stateful%20Functions%22) for an overview of ongoing community work.

## <a name="license"></a>License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
