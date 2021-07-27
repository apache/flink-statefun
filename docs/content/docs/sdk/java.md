---
title: Java
weight: 3
type: docs
aliases:
  - /sdk/java.html
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

# Java SDK

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.

To get started, add the Java SDK as a dependency to your application.

{{< artifact statefun-sdk-java >}}

## Defining A Stateful Function

A stateful function is any class that implements the `StatefulFunction` interface.
In the following example, a `StatefulFunction` maintains a count for every user
of an application, emitting a customized greeting.

```java
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;

public class GreeterFn implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/greeter");

    static final TypeName INBOX = TypeName.typeNameFromString("com.example.fns/inbox");

    static final ValueSpec<Integer> SEEN = ValueSpec.named("seen").withIntType();

    @Override 
    CompletableFuture<Void> apply(Context context, Message message) {
        String name = message.asUtf8String();

        var storage = context.storage();
        var seen = storage.get(SEEN).orElse(0);
        storage.set(SEEN, seen + 1);

        context.send(
            MessageBuilder.forAddress(INBOX, name)
                .withValue("Hello " + name + " for the " + seen + "th time!")
                .build());

        return context.done();
    }
}
```

This code declares a greeter function that will be [registered](#serving-functions) under the logical type name `com.example.fns/greeter`. Type names must take the form `<namesapce>/<name>`.
It contains a single `ValueSpec`, which is implicitly scoped to the current address and stores an integer.

Every time a message is sent to a greeter instance, it is interpreted as a `string` representing the users name.
Both messages and state are strongly typed - either one of the default [built-in types]({{< ref "docs/sdk/appendix#types" >}}) - or a [custom type](#types).

The function finally builds a custom greeting for the user.
The number of times that particular user has been seen so far is queried from the state store and updated
and the greeting is sent to the users' inbox (another function type). 

## Types

Stateful Functions strongly types all messages and state values. 
Because they run in a distributed manner and state values are persisted to stable storage, Stateful Functions aims to provide efficient and easy to use serializers. 

Out of the box, all SDKs offer a set of highly optimized serializers for common primitive types; boolean, numerics, and strings.
Additionally, users are encouraged to plug-in custom types to model more complex data structures. 

In the [example above](#defining-a-stateful-function), the greeter function consumes a simple `string`.
Often, functions need to consume more complex types containing several fields.

By defining a custom type, this object can be passed transparently between functions and stored in state.
And because the type is tied to a logical typename, instead of the physical Java class, it can be passed to functions written in other language SDKs. 

```java
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;
import java.util.Objects;

public class User {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final Type<User> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.example/User"),
            mapper:writeValueAsBytes,
    bytes ->mapper.readValue(bytes,User .class));

    private final String name;

    private final String favoriteColor;

    @JsonCreator
    public User(
            @JsonProperty("name") String name,
            @JsonProperty("favorite_color") String favoriteColor) {

        this.name = Objects.requireNonNull(name);
        this.favoriteColor = Objects.requireNonNull(favoriteColor);
    }

    public String getName() {
        return name;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    @Override
    public String toString() {
        return "User{name=" name + ",favoriteColor=" favoriteColor + "}"
    }
}
```

## State

Stateful Functions treats state as a first class citizen and so all functions can easily define state that is automatically made fault tolerant by the runtime.
State declaration is as simple as defining one or more `ValueSpec`s describing your state values.
Value specifications are defined with a unique (to the function) name and [type](#types).

{{< hint info >}}
All value specifications must be eagerly registered in the `StatefulFuctionSpec` when composing
the applications [RequestReplyHandler](#serving-functions).
{{< /hint >}}

```java
// Value specification for a state named `seen` 
// with the primitive integer type
ValueSpec
    .named("seen")
    .withIntType();

// Value specification with a custom type
ValueSpec
    .name("user")
    .withCustomType(User.TYPE);
```

At runtime, functions can `get`, `set`, and `remove` state values scoped to the address of the current message.

```java
public class SimpleGreeter implements StatefulFunction {
    
	private static final ValueSpec<Integer> SEEN_SPEC = ValueSpec
			.named("seen")
			.withIntType();
    
	@Override
	public CompletableFuture<Void> apply(Context context, Message argument) {
		// Read the current value of the state
		// or 0 if no value is set
		int seen = context.storage().get(SEEN_SPEC).orElse(0);
      	
		seen += 1;
      	
		// Update the state which will
		// be made persistent by the runtime
		context.storage().set(SEEN_SPEC, seen);

		System.out.println("The current count is " + seen);
      
		if (seen > 10) {
			// Delete the state value
			context.storage().remove(SEEN_SPEC);
		}

		return CompletableFuture.completedFuture(null);
	}
}
```

### State Expiration

By default, state values are persisted until manually `remove`d by the user.
Optionally, they may be configured to expire and be automatically deleted after a specified duration.

```java
// Value specification that will automatically
// delete the value if the function instance goes 
// more than 30 minutes without being called
ValueSpec
    .named("seen")
    .thatExpiresAfterCall(Duration.ofDays(1))
    .withIntType();

// Value specification that will automatically
// delete the value if it goes more than 1 day
// without being written
ValueSpec
    .named("seen")
    .thatExpireAfterWrite(Duration.ofDays(1))
    .withIntType();
```

## Sending Delayed Messages

Functions can send messages on a delay so that they will arrive after some duration.
They may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking, so functions will continue to process records between when a delayed message is sent and received.
Additionally, they are fault-tolerant and never lost, even when recovering from failure. 

This example sends a response back to the calling function after a 30 minute delay.

```java
import java.util.concurrent.CompletableFuture;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.message.Message;

public class DelayedFn implements StatefulFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedFn.class);

    static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/delayed");

    @Override 
    CompletableFuture<Void> apply(Context context, Message message) {
        if (!context.caller().isPresent()) {
            LOG.debug("Message has no known caller meaning it was sent directly from an ingress");
            return;
        }

        var caller = context.caller().get();
        context.sendAfter(Duration.ofMinutes(30), MessageBuilder
            .forAddress(caller)
            .withValue("Hello from the future!"));
    }
}
```

## Egress

Functions can message other stateful functions and egresses, exit points for sending messages to the outside world.
As with other messages, egress messages are always well-typed. 
Additionally, they contain metadata pertinent to the specific egress type.

{{< tabs "egress" >}}
{{< tab "Apache Kafka" >}}
```java
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;

public class GreeterFn implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/greeter");

    static final TypeName KAFKA_EGRESS = TypeName.typeNameFromString("com.example/greets");

    static final ValueSpec<Integer> SEEN = ValueSpec.named("seen").withIntType();

    @Override 
    CompletableFuture<Void> apply(Context context, Message message) {
        if (!message.is(User.TYPE)) {
            throw new IllegalStateException("Unknown type");
        }

        User user = message.as(User.TYPE);
        String name = user.getName();

        var storage = context.storage();
        var seen = storage.get(SEEN).orElse(0);
        storage.set(SEEN, seen + 1);

        context.send(
            KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                .withTopic("greetings")
                .withUtf8Key(name)
                .withUtf8Value("Hello " + name + " for the " + seen + "th time!")
                .build());

        return context.done();
    }
}
```
{{< /tab >}}
{{< tab "Amazon Kinesis" >}}
```java
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.io.KinesisEgressMessage;

public class GreeterFn implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/greeter");

    static final TypeName KINESIS_EGRESS = TypeName.typeNameFromString("com.example/greets");

    static final ValueSpec<Integer> SEEN = ValueSpec.named("seen").withIntType();

    @Override 
    CompletableFuture<Void> apply(Context context, Message message) {
        if (!message.is(User.TYPE)) {
            throw new IllegalStateException("Unknown type");
        }

        User user = message.as(User.TYPE);
        String name = user.getName();

        var storage = context.storage();
        var seen = storage.get(SEEN).orElse(0);
        storage.set(SEEN, seen + 1);

        context.send(
            KinesisEgressMessage.forEgress(KINESIS_EGRESS)
                .withStream("greetings")
                .withUtf8PartitionKey(name)
                .withUtf8Value("Hello " + name + " for the " + seen + "th time!")
                .build());

        return context.done();
    }
}
```
{{< /tab >}}
{{< /tabs >}}

## Serving Functions

The Java SDK ships with a ``RequestReplyHandler`` that automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The handler is composed of multiple `StatefulFunctionSpec`'s which describe all the `StatefulFunction` classes defined within the application.
The specification contains the functions logical type name, all state value specifications, and a supplier to create an instance of the Java class.

Once built, the ``RequestReplyHandler`` may be exposed using any HTTP framework like [Spring Boot](https://spring.io/projects/spring-boot), [Micronaught](https://micronaut.io/), [Quarkus](https://quarkus.io/), [Dropwizard](https://www.dropwizard.io/en/latest/), [Vertx](https://vertx.io/), or just bare bones [Netty](https://netty.io/).
This example create a handler for greeter function and exposes it using the [Undertow](https://undertow.io/) web framework. 

```java
import static io.undertow.UndertowOptions.ENABLE_HTTP2;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

public class UndertowMain {

	public static void main(String... args) {
		StatefulFunctionSpec spec =
			StatefulFunctionSpec.builder(GreeterFn.TYPE)
				.withValueSpec(GreeterFn.SEEN)
				.withSupplier(GreeterFn::new)
				.build();

		// obtain a request-reply handler based on the spec above
		StatefulFunctions functions = new StatefulFunctions();
		functions.withStatefulFunction(spec);
		RequestReplyHandler handler = functions.requestReplyHandler();

		// this is a generic HTTP server that hands off the request-body
		// to the handler above and visa versa.
		Undertow server =
			Undertow.builder()
				.addHttpListener(5000, "0.0.0.0")
				.setHandler(new UndertowHttpHandler(handler))
				.setServerOption(ENABLE_HTTP2, true)
				.build();

		server.start();
	}

	private static final class UndertowHttpHandler implements HttpHandler {

		private final RequestReplyHandler handler;

		UndertowHttpHandler(RequestReplyHandler handler) {
			this.handler = Objects.requireNonNull(handler);
		}

		@Override
		public void handleRequest(HttpServerExchange exchange) {
			exchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
		}

		private void onRequestBody(HttpServerExchange exchange, byte[] requestBytes) {
			exchange.dispatch();
			CompletableFuture<Slice> future = handler.handle(Slices.wrap(requestBytes));
			future.whenComplete((response, exception) -> onComplete(exchange, response, exception));
		}

		private void onComplete(HttpServerExchange exchange, Slice responseBytes, Throwable ex) {
			if (ex != null) {
				ex.printStackTrace(System.out);
				exchange.getResponseHeaders().put(Headers.STATUS, 500);
				exchange.endExchange();
				return;
			}
			exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
			exchange.getResponseSender().send(responseBytes.asReadOnlyByteBuffer());
		}
	}
}
```

## Next Steps

Keep learning with information on setting up [I/O modules]({{< ref "docs/io-module/overview" >}}) and configuring the [Stateful Functions runtime]({{< ref "docs/deployment/overview" >}}).
