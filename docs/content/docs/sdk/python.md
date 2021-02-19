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
The Python SDK is supported as a [remote module]({{< ref "docs/sdk/overview#remote-module" >}}).

To get started, add the Python SDK as a dependency to your application.

{{< selectable >}}
```bash
apache-flink-statefun=={{< version >}}
```
{{< /selectable >}}

## Defining A Stateful Function

A stateful function is any function that that takes two parameters, a ``context`` and ``message``.
The function is bound to the runtime through the stateful functions decorator.
The following is an example of a simple hello world function.

```python
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/hello")
def hello_function(context, message):
    """A simple hello world function"""
    user = User()
    message.Unpack(user)

    print("Hello " + user.name)
```

This code declares a function with in the namespace ``example`` and of type ``hello`` and binds it to the ``hello_function`` Python instance.

Messages's are untyped and passed through the system as ``google.protobuf.Any`` so one function can potentially process multiple types of messages.

The ``context`` provides metadata about the current message and function, and is how you can call other functions or external systems.
A full reference of all methods supported by the context object are listed at the [bottom of this page](#context-reference).

## Type Hints

If the function has a static set of known supported types, they may be specified as [type hints](https://docs.python.org/3/library/typing.html).
This includes [union types](https://docs.python.org/3/library/typing.html#typing.Union) for functions that support multiple input message types.

```python
import typing
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/hello")
def hello_function(context, message: User):
    """A simple hello world function with typing"""

    print("Hello " + message.name)

@function.bind("example/goodbye")
def goodbye_function(context, message: typing.Union[User, Admin]):
    """A function that dispatches on types"""

    if isinstance(message, User):
        print("Goodbye user")
    elif isinstance(message, Admin):
        print("Goodbye Admin")
```

## Function Types and Messaging

The decorator ``bind`` registers each function with the runtime under a function type.
The function type must take the form ``<namespace>/<name>``.
Function types can then be referenced from other functions to create an address and message a particular instance.

```python
from google.protobuf.any_pb2 import Any
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/caller")
def caller_function(context, message):
    """A simple stateful function that sends a message to the user with id `user1`"""

    user = User()
    user.user_id = "user1"
    user.name = "Seth"

    envelope = Any()
    envelope.Pack(user)

    context.send("example/hello", user.user_id, envelope)
```

Alternatively, functions can be manually bound to the runtime.

```python
functions.register("example/caller", caller_function)
```
  
## Sending Delayed Messages

Functions are able to send messages on a delay so that they will arrive after some duration.
Functions may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking so functions will continue to process records between the time a delayed message is sent and received.
The delay is specified via a [Python timedelta](https://docs.python.org/3/library/datetime.html#datetime.timedelta).

```python
from datetime import timedelta
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/delay")
def delayed_function(context, message):
    """A simple stateful function that sends a message to its caller with a delay"""

    response = Response()
    response.message = "hello from the past"

    context.pack_and_send_after(
        context.caller.typename(), 
        context.caller.identity,
        timedelta(minutes=30),
        response)
```

## Persistence

Stateful Functions treats state as a first class citizen and so all stateful functions can easily define state that is automatically made fault tolerant by the runtime.
All stateful functions may contain state by merely storing values within the ``context`` object.
The data is always scoped to a specific function type and identifier.
State values could be absent, ``None``, or a ``google.protobuf.Any``.

{{< hint info >}}
[Remote modules]({{< ref "docs/sdk/overview#embedded-module" >}}) require that all state values are eagerly registered at module.yaml.
It'll also allow configuring other state properties, such as state expiration. Please refer to that page for more details.
{{< /hint >}}
    
Below is a stateful function that greets users based on the number of times they have been seen.

```python
from google.protobuf.any_pb2 import Any
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/count")
def count_greeter(context, message):
    """Function that greets a user based on
    the number of times it has been called"""
    user = User()
    message.Unpack(user)


    state = context["count"]
    if state is None:
        state = Any()
        state.Pack(Count(1))
        output = generate_message(1, user)
    else:
        counter = Count()
        state.Unpack(counter)
        counter.value += 1
        output = generate_message(counter.value, user)
        state.Pack(counter)

    context["count"] = state
    print(output)

def generate_message(count, user):
    if count == 1:
        return "Hello " + user.name
    elif count == 2:
        return "Hello again!"
    elif count == 3:
        return "Third time's the charm"
    else:
        return "Hello for the " + count + "th time"
```

Additionally, persisted values may be cleared by deleting its value.

```python
del context["count"]
```

## Exposing Functions

The Python SDK ships with a ``RequestReplyHandler`` that automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The ``RequestReplyHandler`` may be exposed using any HTTP framework.

```python
from statefun import RequestReplyHandler

handler RequestReplyHandler(functions)
```
    

### Serving Functions With Flask

One popular Python web framework is [Flask](https://palletsprojects.com/p/flask/).
It can be used to quickly and easily expose a ``RequestReplyHandler``.

```python
@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
	app.run()
```


### Serving Asynchronous Functions 

The Python SDK ships with an additional handler, ``AsyncRequestReplyHandler``, that supports Python's awaitable functions (coroutines).
This handler can be used with asynchronous Python frameworks, for example [aiohttp](https://docs.aiohttp.org/en/stable/).

```python
@functions.bind("example/hello")
async def hello(context, message):
    response = await compute_greeting(message)
    context.reply(response)

from aiohttp import web

handler = AsyncRequestReplyHandler(functions)

async def handle(request):
    req = await request.read()
    res = await handler(req)
    return web.Response(body=res, content_type="application/octet-stream")

app = web.Application()
app.add_routes([web.post('/statefun', handle)])

if __name__ == '__main__':
    web.run_app(app, port=5000)

```


## Context Reference

The ``context`` object passed to each function has the following attributes / methods.

* address 
    * The address of the current function under execution
* caller
    * The address of the function that sent the current message. May be ``None`` if the message came from an ingress.
* send(self, typename: str, id: str, message: Any)
    * Send a message to any function with the function type of the the form ``<namesapce>/<type>`` and message of type ``google.protobuf.Any``
* pack_and_send(self, typename: str, id: str, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* reply(self, message: Any)
    * Sends a message to the invoking function
* pack_and_reply(self, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* send_after(self, delay: timedelta, typename: str, id: str, message: Any)
    * Sends a message after a delay
* pack_and_send_after(self, delay: timedelta, typename: str, id: str, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* send_egress(self, typename, message: Any)
    * Emits a message to an egress with a typename of the form ``<namespace>/<name>``
* pack_and_send_egress(self, typename, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* \_\_getitem\_\_(self, name)
    * Retrieves the state registered under the name as an ``Any`` or ``None`` if no value is set
* \_\_delitem\_\_(self, name)
    * Deletes the state registered under the name
* \_\_setitem\_\_(self, name, value: Any)
    * Stores the value under the given name in state.
