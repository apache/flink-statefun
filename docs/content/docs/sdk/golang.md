---
title: Golang
weight: 4
type: docs
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

# Golang SDK

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.

To get started, add the Golang SDK as a dependency to your application.

{{< selectable >}}
```
require github.com/apache/flink-statefun/statefun-sdk-go/v3 v{{< version >}}
```
{{< /selectable >}}

## Defining A Stateful Function

A stateful function is any class that implements the `StatefulFunction` interface.
In the following example, a `StatefulFunction` maintains a count for every user
of an application, emitting a customized greeting.

```go
import (
	"fmt"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

type Greeter struct {
	SeenCount statefun.ValueSpec
}

func (g *Greeter) Invoke(ctx statefun.Context, message statefun.Message) error {
	if !message.Is(statefun.StringType) {
		return fmt.Errorf("unexpected message type %s", message.ValueTypeName())
	}

	var name string
	_ = message.As(statefun.StringType, &name)

	storage := ctx.Storage()

	var count int32
	storage.Get(g.SeenCount, &count)

	count += 1

	storage.Set(g.SeenCount, count)

	ctx.Send(statefun.MessageBuilder{
		Target: statefun.Address{
			FunctionType: statefun.TypeNameFrom("com.example.fns/inbox"),
			Id:           name,
		},
		Value: fmt.Sprintf("Hello %s for the %dth time!", name, count),
	})

	return nil
}
```

This code declares a greeter function that will be [registered](#serving-functions) under the logical type name `com.example.fns/greeter`. Type names must take the form `<namesapce>/<name>`.
It contains a single `ValueSpec`, which is implicitly scoped to the current address and stores an int32.

Alternatively, a stateful function can be defined as a function pointer.

```go
func greeter(ctx statefun.Context, message statefun.Message) error {
    panic("Implement me!")
}
```

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
And because the type is tied to a logical typename, instead of the physical golang function or struct, it can be passed to functions written in other language SDKs. 

```go
import (
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

type User struct {
	Name          string
	FavoriteColor string
}

UserType = MakeJsonType(statefun.TypeNameFrom("com.example/User"))
```

Alternatively, you can implement the `SimpleType` interface for full control over serialization.

## State

Stateful Functions treats state as a first class citizen and so all functions can easily define state that is automatically made fault tolerant by the runtime.
State declaration is as simple as defining one or more `ValueSpec`s describing your state values.
Value specifications are defined with a unique (to the function) name and [type](#types).

{{< hint info >}}
All value specifications must be eagerly registered in the `StatefulFuctions` decorator when declaring the function.
{{< /hint >}}

```go
// Value specification for a state named `seen` 
// with the primitive integer type
statefun.ValueSpec {
    Name:      "seen_count",
    ValueType: statefun.Int32Type,
}

// Value specification with a custom type
statefun.ValueSpec {
    Name:      "user",
    ValueType: UserType,
}
```

At runtime, functions can `get`, `set`, and `remove` state values scoped to the address of the current message.
The value spec is scoped within the `Greeter` struct for convenience.

```go
type Greeter struct {
	SeenCount statefun.ValueSpec
}

func (g *Greeter) Invoke(ctx statefun.Context, message statefun.Message) error {
    storage := ctx.Storage()

    // Read the current value of the state
    // or zero value if no value is set
    var count int32
    storage.Get(g.SeenCount, &count)

    count += 1

    // Update the state which will
    // be made persistent by the runtime
    storage.Set(g.SeenCount, count)

    log.Printf("the current count is %s", count)
    
    if count > 10 {
        // Delete the state value
        storage.Remove(g.SeenCount)
    }

    return nil
}
```

### State Expiration

By default, state values are persisted until manually `deleted`ed by the user.
Optionally, they may be configured to expire and be automatically deleted after a specified duration.

```go
import "time"

// Value specification that will automatically
// delete the value if the function instance goes 
// more than 30 minutes without being called
statefun.ValueSpec {
    Name:      "seen_count",
    ValueType: statefun.Int32Type,
    Expiration: ExpireAfterCall(time.Duration(30) * time.Minutes)
}

// Value specification that will automatically
// delete the value if it goes more than 30 minutes
// without being written
statefun.ValueSpec {
    Name:      "seen_count",
    ValueType: statefun.Int32Type,
    Expiration: ExpireAfterWrite(time.Duration(30) * time.Minutes)
}
```

## Sending Delayed Messages

Functions can send messages on a delay so that they will arrive after some duration.
They may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking, so functions will continue to process records between when a delayed message is sent and received.
Additionally, they are fault-tolerant and never lost, even when recovering from failure. 

This example sends a response back to the calling function after a 30 minute delay.

```go
import (
    "fmt"
    "github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
    "time"
)

func delayed(ctx statefun.Context, message: statefun.Message) error {
    if ctx.Caller() == nil {
        fmt.Println("message has no known caller meaning it was sent directly from an ingress")
    }

    ctx.SendAfter(
        time.Duration(30) * time.Minutes,
        MessageBuilder {
            Target: ctx.Caller(),
            Value: "Hello from the future!",
        }
    )

    return nil
}
```

## Egress

Functions can message other stateful functions and egresses, exit points for sending messages to the outside world.
As with other messages, egress messages are always well-typed. 
Additionally, they contain metadata pertinent to the specific egress type.

{{< tabs "egress" >}}
{{< tab "Apache Kafka" >}}
```go
import (
    "fmt"
    "github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
    "time"
)

func (g *Greeter) Invoke(ctx statefun.Context, message: statefun.Message) error {
    if !message.Is(UserType) {
        return fmt.Errorf("unknown type %s", message.ValueTypeName())
    }

    var user User
    _ = user.As(UserType, &user)

    storage = context.Storage()

    var seen int32
    storage.Get(g.SeenCount, &seen)
    seen += 1
    storage.Set(g.SeenCount, seen)

	ctx.SendEgress(&statefun.KafkaEgressBuilder{
		Target: statefun.TypeNameFrom("com.example/greets"),
		Topic:  "greetings",
		Key:    user.Name,
		Value:  fmt.Sprintf("Hello %s for the %s-th time!", user.Name, count),
	})

    return nil
}
```
{{< /tab >}}
{{< tab "Amazon Kinesis" >}}
```go
import (
    "fmt"
    "github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
    "time"
)

func (g *Greeter) Invoke(ctx statefun.Context, message: statefun.Message) error {
    if !message.Is(UserType) {
        return fmt.Errorf("unknown type %s", message.ValueTypeName())
    }

    var user User
    _ = user.As(UserType, &user)

    storage = context.Storage()

    var seen int32
    storage.Get(g.SeenCount, &seen)
    seen += 1
    storage.Set(g.SeenCount, seen)

	ctx.SendEgress(&statefun.KinesisEgressBuilder{
		Target: statefun.TypeNameFrom("com.example/greets"),
		Stream:  "greetings",
		PartitionKey:    user.Name,
		Value:  fmt.Sprintf("Hello %s for the %s-th time!", user.Name, count),
	})

    return nil
}
```
{{< /tab >}}
{{< /tabs >}}

## Serving Functions

The Golang SDK ships with a ``RequestReplyHandler`` that is a standard http `Handler` and automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The handler is created using the `StatefulFunctionsBuilder` and is composed of all the stateful functions bound to the system.

This example create a handler for greeter function and exposes it using the standard golang web framework. 

```go
import (
    "fmt"
    "github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
    "net/http"
)

func main() {
	greeter := &Greeter{
		SeenCount: statefun.ValueSpec{
			Name:      "seen_count",
			ValueType: statefun.Int32Type,
		},
	}

	builder := statefun.StatefulFunctionsBuilder()
	_ = builder.WithSpec(statefun.StatefulFunctionSpec{
		FunctionType: statefun.TypeNameFrom("com.example.fns/greeter"),
		States:       []statefun.ValueSpec{greeter.SeenCount},
		Function:     greeter,
	})

	http.Handle("/statefun", builder.AsHandler())
	_ = http.ListenAndServe(":8000", nil)
}
```

## Next Steps

Keep learning with information on setting up [I/O components]({{< ref "docs/modules/io/overview" >}}) and configuring the [Stateful Functions runtime]({{< ref "docs/deployment/overview" >}}).
