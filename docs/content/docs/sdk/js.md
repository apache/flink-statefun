---
title: JavaScript
weight: 2
type: docs
aliases:
  - /sdk/js.html
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

# JavaScript SDK

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.

To get started, add the JavaScript SDK as a dependency to your application.

{{< selectable >}}
```
npm install apache-flink-statefun@{{< version >}}
```
{{< /selectable >}}

## Defining a Stateful Function

A stateful function is any function that takes a `context` and `message` parameter.
In the following example, a `StatefulFunction` maintains a count for every user
of an application, emitting a customized greeting.

```javascript
const {messageBuilder, StateFun, Context} = require("apache-flink-statefun");

let statefun = new StateFun();

statefun.bind({
    typename: "com.example.fns/greeter",
    fn(context, message) {
        const name = message.asString();
        let seen = context.storage.seen || 0;
        seen = seen + 1;
        context.storage.seen = seen;
        
        context.send(
            messageBuilder({typename: 'com.example.fns/inbox',
                            id: name,
                            value: `"Hello ${name} for the ${seen}th time!`})
        );
    },
    specs: [{
        name: "seen",
        type: StateFun.intType(),
    }
    ]
});
```

This code declares a greeter function that will be [registered](#serving-functions) under the logical type name `com.example.fns/greeter`. Type names must take the form `<namesapce>/<name>`.
It contains a single `ValueSpec`, which is implicitly scoped to the current address and stores an integer.

Every time a message is sent a greeter instance, it is interpreted as a `string` representing the users name.
Both messages and state are strongly typed - either one of the default [built-in types]({{< ref "docs/sdk/appendix#types" >}}) - or a [custom type](#types).

The function finally builds a custom greeting for the user.
The number of times that particular user has been seen so far is queried from the state store and updated
and the greeting is sent to the user's inbox (another function type). 

## Types

Stateful Functions strongly types all messages and state values. 
Because they run in a distributed manner and state values are persisted to stable storage, Stateful Functions aims to provide efficient and easy to use serializers. 

Out of the box, all SDKs offer a set of highly optimized serializers for common primitive types; boolean, numerics, and strings.
Additionally, users are encouraged to plug-in custom types to model more complex data structures. 

In the [example above](#defining-a-stateful-function), the greeter function consumes a simple `string`.
Often, functions need to consume more complex types containing several fields.

By defining a custom type, this object can be passed transparently between functions and stored in state.
And because the type is tied to a logical typename, instead of the physical Javascript type, it can be passed to functions written in other language SDKs. 

```javascript
const {StateFun} = require("apache-flink-statefun");

let statefun = new StateFun();

UserType = StateFun.jsonType("com.example/User");
```

## State

Stateful Functions treats state as a first class citizen and so all functions can easily define state that is automatically made fault tolerant by the runtime.
State declaration is as simple as defining one or more `ValueSpec`s describing your state values.
Value specifications are defined with a unique (to the function) name and [type](#types).

{{< hint info >}}
All value specifications must be eagerly registered when binding the functions.
{{< /hint >}}

```javascript
// Value specification for a state named `seen` 
// with the primitive integer type
ValueSpec.fromOpts({name: "seen_count", type: StateFun.intType()});

// Value specification with a custom type
ValueSpec.fromOpts({name: "user", type: UserType});
```

At runtime, functions can access state values scoped to the address of the current message.

```javascript
const {messageBuilder, StateFun, Context} = require("apache-flink-statefun");

let statefun = new StateFun();

statefun.bind({
    typename: "com.example.fns/greeter",
    fn(context, message) {
        let seen = context.storage.seen_count || 0;
        seen += 1;
        // Update the state which will
        // be made persistent by the runtime
        context.storage.seen_count = seen;
        
        console.log(`The current count is ${seen}`);
        
        if (seen > 10) {
            context.storage.seen_count = null;
        }
    },
    specs: [{
        name: "seen_count",
        type: StateFun.intType(),
    }
    ]
});
```

### State Expiration

By default, state values are persisted until manually `deleted`ed by the user.
Optionally, they may be configured to expire and be automatically deleted after a specified duration.

```javascript
// Value specification that will automatically
// delete the value if the function instance goes 
// more than 30 minutes without being called

ValueSpec.fromOpts({name: "seen_count", type: StateFun.intType(), expireAfterCall: 30 * 1000 * 60});
```

## Sending Delayed Messages

Functions can send messages on a delay so that they will arrive after some duration.
They may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking, so functions will continue to process records between when a delayed message is sent and received.
Additionally, they are fault-tolerant and never lost, even when recovering from failure. 

This example sends a message after a 30-minute delay.

```javascript
const {StateFun, messageBuilder} = require("apache-flink-statefun");

let statefun = new StateFun();

statefun.bind({
    typename: "com.example.fns/greeter",
    fn(context, message) {
        context.sendAfter(30 * 60 * 1000,
            messageBuilder({
                typename: 'com.example.fns/inbox',
                id: 'foo-bar-baz',
                value: 'Hello!'
            }));
    }
});
```

## Egress

Functions can message other stateful functions and egresses, exit points for sending messages to the outside world.
As with other messages, egress messages are always well-typed. 
Additionally, they contain metadata pertinent to the specific egress type.

{{< tabs "egress" >}}
{{< tab "Apache Kafka" >}}
```javascript
const {StateFun, kafkaEgressMessage} = require("apache-flink-statefun");

let statefun = new StateFun();

statefun.bind({
    typename: "com.example.fns/greeter",
    fn(context, message) {
        context.send(
            kafkaEgressMessage({
                typename: 'com.example/greets',
                topic: 'greetings',
                key: 'foo',
                value: 'bar'            
            }));
    }
});
```
{{< /tab >}}
{{< tab "Amazon Kinesis" >}}
```javascript
const {StateFun, kinesisEgressMessage} = require("apache-flink-statefun");

let statefun = new StateFun();

statefun.bind({
    typename: "com.example.fns/greeter",
    fn(context, message) {
        context.send(
            kinesisEgressMessage({
                typename: 'com.example/greets',
                stream: 'greetings',
                partitionKey: 'foo',
                value: 'bar'            
            }));
    }
});
```
{{< /tab >}}
{{< /tabs >}}

## Serving Functions

The JavaScript SDK for NodeJs, ships with a ``Handler`` that automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The handler is composed of all the stateful functions bound to the `StateFun` instance.

```javascript
const http = require("http");
const {StateFun} = require("apache-flink-statefun");

let statefun = new StateFun();

statefun.bind({
    typename: "example/command-interpreter-fn",
    fn(context, message) {
        console.log(`Hello there ${message.asString()}`);        
    }
});

http.createServer(statefun.handler()).listen(8000);
```

## Next Steps

Keep learning with information on setting up [I/O components]({{< ref "docs/modules/io/overview" >}}) and configuring the [Stateful Functions runtime]({{< ref "docs/deployment/overview" >}}).
