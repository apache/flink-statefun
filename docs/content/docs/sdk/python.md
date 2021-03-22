---
title: Python
weight: 2
type: docs
aliases:
  - /sdk/python.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Python SDK

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.

To get started, add the Python SDK as a dependency to your application.

{{< selectable >}}
```
apache-flink-statefun=={{< version >}}
```
{{< /selectable >}}

## Defining A Stateful Function

A stateful function is any function that takes a `context` and message parameter.
In the following example, a `StatefulFunction` maintains a count for every user
of an application, emiting a customized greeting.

```python
from statefun import *

functions = StatefulFunctions()


@functions.bind(
    typename='com.example.fns/greeter',
    specs=[ValueSpec(name='seen_count', type=IntType)])
async def greet(ctx: Context, message: Message):
    name = message.as_string()

    storage = ctx.storage
    seen = storage.seen_count or 0
    storage.seen_count = seen + 1

    ctx.send(
        message_builder(target_typename='com.example.fns/inbox',
                        target_id=name,
                        str_value=f"Hello {name} for the {seen}th time!"))
```

This code declares a greeter function that will be [registered](#exposing-functions) under the logical type name `com.example.fns/greeter`. Type names must take the form `<namesapce>/<name>`.
It contains a single `ValueSpec`, which is implicitly scoped to the current address and stores an integer.

Every time a message is sent a greeter instance, it is interpreted as a `string` represting the users name.
Both messages and state are strongly typed - either one of the default [built-in types]({{< ref "docs/sdk/appendix#types" >}}) - or a [custom type](#types).

The function finally builds a custom greeting for the user.
The number of times that particular user has been seen so far is queried from the state store and updated
and the greeting is sent to the users' inbox (another function type). 

## Types

Stateful Functions strongly types all messages and state values. 
Because they run in a distributed manner and state values are persisted to stable storage, Stateful Functions aims to provide efficient and easy to user serializers. 

Out of the box, all SDKs offer a set of highly optimized serializers for common primitive types; boolean, numerics, and strings.
Additionally, users are encouraged to plug-in custom types to model more complex data structures. 

In the [example above](#defining-a-stateful-function), the greeter function consumes a simple `string`.
Often, functions need to consume more complex types containing several fields.

By defining a custom type, this object can be passed transparently between functions and stored in state.
And because the type is tied to a logical typename, instead of the physical Python class, it can be passed to functions written in other langauge SDKs. 

```python
from statefun import *
import json


class User:
    def __init__(self, name, favorite_color):
        self.name = name
        self.favorite_color = favorite_color


UserType = simple_type(
    typename="com.example/User",
    serialize_fn=lambda user: json.dumps(user.__dict__),
    deserialize_fn=lambda serialized: User(**json.loads(serialized)))
```

## State

Stateful Functions treats state as a first class citizen and so all functions can easily define state that is automatically made fault tolerant by the runtime.
State declaration is as simple as defining one or more `ValueSpec`'s describing your state values.
Value specifications are defined with a unique (to the function) name and [type](#types).

{{< hint info >}}
All value specificiations must be earerly registered in the `StatefulFuctions` decorator when declaring the function.
{{< /hint >}}

```python
# Value specification for a state named `seen` 
# with the primitive integer type
ValueSpec(name='seen_count', type=IntType)

# Value specification with a custom type
ValueSpec(name='user', type=UserType)
```

At runtime, functions can `get`, `set`, and `delete` state values scoped to the address of the current message.

```python
@functions.bind(
    typename='com.example.fns/greeter',
    specs=[ValueSpec(name='seen_count', type=IntType)])
async def greet(ctx: Context, message: Message):
    storage = ctx.storage
    
    # Read the current value of the state
    # or None if no value is set
    count = storage.seen_count or 0
    count += 1
    
    # Update the state which will
    # be made persistent by the runtime
    storage.seen_count = count
    
    print(f"the current count is {count}")
    
    if count > 10:
        
        # Delete the state value
        del storage.seen_count
```

### State Expiration

By default, state values are persisted until manually `deleted`ed by the user.
Optionally, they may be configured to expire and be automatically deleted after a specified duration.

```python
from datetime import timedelta

# Value specification that will automatically
# delete the value if the function instance goes 
# more than 30 minutes without being called
ValueSpec(name='seen_count', type=IntType, expire_after_call=timedelta(minutes=30))

# Value specification that will automatically
# delete the value if it goes more than 30 minutes
# without being written
ValueSpec(name='seen_count', type=IntType, expire_after_write=timedelta(minutes=30))
```

## Sending Delayed Messages

Functions can send messages on a delay so that they will arrive after some duration.
They may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking, so functions will continue to process records between when a delayed message is sent and received.
Additionally, they are fault-tolerant and never lost, even when recovering from failure. 

This example sends a response back to the calling function after a 30 minute delay.

```python
from statefun import *

from datetime import timedelta

functions = StatefulFunctions()


@functions.bind(typename='com.example.fns/delayed')
async def delayed(ctx: Context, message: Message):
    if ctx.caller is None:
        print('Message has no known caller meaning it was sent directly from an ingress')

    ctx.send_after(
        duration=timedelta(minutes=30),
        message=message_builder(
            target_typename=ctx.caller.typename,
            target_id=ctx.caller.id,
            str_value='Hello from the future!'))
```

## Egress

Functions can message other stateful functions and egresses, exit points for sending messages to the outside world.
As with other messages, egress messages are always well-typed. 
Additionally, they contain metadata pertinent to the specific egress type.

{{< tabs "egress" >}}
{{< tab "Apache Kafka" >}}
```python
from statefun import *

functions = StatefulFunctions()


@functions.bind(
    typename='com.example.fns/greeter',
    specs=[ValueSpec(name='seen_count', type=IntType)])
async def greet(context, message):
    if not message.is_type(UserType):
        raise ValueError('Unknown type')

    user = message.as_type(UserType)
    name = user.name

    storage = context.storage
    seen = storage.seen_count or 0
    storage.seen_count = seen + 1

    context.send_egress(kafka_egress_message(
        typename='com.example/greets',
        topic='greetings',
        key=name,
        value=f"Hello {name} for the {seen}th time!"))
```
{{< /tab >}}
{{< tab "Amazon Kinesis" >}}
```python
from statefun import *

functions = StatefulFunctions()


@functions.bind(
    typename='com.example.fns/greeter',
    specs=[ValueSpec(name='seen_count', type=IntType)])
async def greet(context, message):
    if not message.is_type(UserType):
        raise ValueError('Unknown type')

    user = message.as_type(UserType)
    name = user.name

    storage = context.storage
    seen = storage.seen_count or 0
    storage.seen_count = seen + 1

    context.send_egress(kinesis_egress_message(
        typename='com.example/greets',
        stream='greetings',
        partition_key=name,
        value=f"Hello {name} for the {seen}th time!"))
```
{{< /tab >}}
{{< /tabs >}}

## Exposing Functions

The Python SDK ships with a ``RequestReplyHandler`` that automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The handler is composed of all the stateful functions bound to the `StatefulFunctions` decorator.

Once built, the ``RequestReplyHandler`` may be exposed using any HTTP framework.
This example create a handler for greeter function and exposes it using the [AIOHTTP](https://docs.aiohttp.org/en/stable/) web framework. 

```python
from statefun import *
from aiohttp import web

functions = StatefulFunctions()


@functions.bind(
    typename='com.example.fns/greeter',
    specs=[ValueSpec(name='seen_count', type=IntType)])
async def greet(ctx: Context, message: Message):
    pass


handler = RequestReplyHandler(functions)


async def handle(request):
    req = await request.read()
    res = await handler.handle_async(req)
    return web.Response(body=res, content_type="application/octet-stream")


app = web.Application()
app.add_routes([web.post('/statefun', handle)])

if __name__ == '__main__':
    web.run_app(app, port=8000)
```