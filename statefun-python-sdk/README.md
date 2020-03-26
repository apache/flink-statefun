# Apache Flink Stateful Functions

Stateful Functions is an [Apache Flink](https://flink.apache.org/) library for distributed applications and services, based on, well, you guessed it: stateful functions.

The project aims to simplify the development of distributed stateful applications by solving some of the common
challenges in those applications: scaling, consistent state management, reliable interaction between distributed
services, and resource management.

Stateful Functions uses Apache Flink for distributed coordination, state, and communication.

This description is meant as a brief walkthrough on the core concepts and how to set things up
to get yourself started with Stateful Functions.

For a fully detailed documentation, please visit the [official docs](https://ci.apache.org/projects/flink/flink-statefun-docs-release-stable).

For code examples, please visit the examples in the [Github repo](https://github.com/apache/flink-statefun/tree/master/statefun-examples).

## Table of Contents

- [Core Concepts](#core-concepts)
   * [Abstraction](#abstraction)
   * [Function modules and extensibility](#modules)
- [Python SDK Overview](#sdkoverview)
- [Contributing](#contributing)
- [License](#license)

## <a name="core-concepts"></a>Core Concepts

### <a name="abstraction"></a>Abstraction

A Stateful Functions application consists of the following primitives: stateful functions, ingresses,
routers, and egresses.

#### Stateful functions

* Stateful functions are the building blocks and namesake of the Stateful Functions framework.
A function is a small piece of logic that are invoked through a message.

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

If you know Apache Flink’s `DataStream` API, you can think of stateful functions a bit like a lightweight
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

## <a name="sdkoverview"></a> Python SDK Overview

### Background

The JVM based stateful functions implementation, has a `RequestReply` extension (a protocol and an implementation) that allows calling into any HTTP endpoint that implements that protocol.

Although it is possible to implement this protocol independently, this is a minimal library for the Python programing language, that:

* Allows users to define and declare their functions in a convenient way
* Dispatch an invocation request sent from the JVM to the appropriate function previously declared

### A Mini Tutorial

#### Define and Declare a function

```
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("demo/greeter")
def greet(context, message: LoginEvent):
    print("Hey " + message.user_name)
```

This code, declares a function with a `FunctionType("demo", "greeter")` and binds the greet Python instance to it.

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

The request reply protocol, requires that the state names would be registered in the module YAML file
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

There are many possible ways to enhance the Stateful Functions API for different types of applications. The runtime and operations will also evolve with the developments in Apache Flink. If you find these ideas interesting or promising, try Stateful Functions out and get involved! 

You can learn more about how to contribute in the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section and check the _Stateful Functions_ component in [Jira](https://issues.apache.org/jira/browse/FLINK-15969?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20%22Stateful%20Functions%22) for an overview of ongoing community work.

## <a name="license"></a>License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
