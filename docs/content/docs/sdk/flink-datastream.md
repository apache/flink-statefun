---
title: "Flink DataStream"
weight: 1002
type: docs
aliases:
  - /sdk/flink-datastream.html
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

# SDK for Flink DataStream Integration

This SDK may be used if you want your Stateful Functions application to consume events from, or output events to
Flink [DataStreams](https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html). Using this SDK,
you may combine pipelines written with the Flink DataStream API or higher-level libraries (such as [Table API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/),
[CEP](https://ci.apache.org/projects/flink/flink-docs-stable/dev/libs/cep.html#flinkcep-complex-event-processing-for-flink) etc.,
basically anything that produces a ``DataStream``) with the programming constructs provided by Stateful Functions to build
complex streaming applications.

To use this, add the Flink DataStream Integration SDK as a dependency to your application:

{{< artifact statefun-flink-datastream >}}

## SDK Overview

The following sections covers the important parts on getting started with the SDK. For the full code and working
example, please take a look at this [example](https://github.com/apache/flink-statefun/blob/master/statefun-examples/statefun-flink-datastream-example/src/main/java/org/apache/flink/statefun/examples/datastream/Example.java).

### Preparing a DataStream Ingress

All ``DataStream``s used as ingresses must contain stream elements of type ``RoutableMessage``. Each ``RoutableMessage``
carries information about the target function's address alongside the input event payload.

You can use the ``RoutableMessageBuilder`` to transform your ``DataStream``s:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> names = env.addSource(...)

DataStream<RoutableMessage> namesIngress =
    names.map(name ->
        RoutableMessageBuilder.builder()
            .withTargetAddress(new FunctionType("example", "greet"), name)
            .withMessageBody(name)
            .build()
    );
```

In the above example, we transformed a ``DataStream<String>`` into a ``DataStream<RoutableMessage>`` by mapping
element in the original stream to a ``RoutableMessage``, with each element targeted for the function type ``(example:greet)``.

### Binding Functions, Ingresses, and Egresses

Once you have transformed your stream ingresses, you may start binding functions to consume the stream events, as well
as DataStream egresses to produce the outputs to:

```java
FunctionType GREET = new FunctionType("example", "greet");
FunctionType REMOTE_GREET = new FunctionType("example", "remote-greet");
EgressIdentifier<String> GREETINGS = new EgressIdentifier<>("example", "greetings", String.class);

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> names = env.addSource(...);

DataStream<RoutableMessage> namesIngress =
    names.map(name ->
        RoutableMessageBuilder.builder()
            .withTargetAddress(GREET, name)
            .withMessageBody(name)
            .build()
    );

StatefulFunctionEgressStreams egresses =
    StatefulFunctionDataStreamBuilder.builder("example")
        .withDataStreamAsIngress(namesIngress)
        .withRequestReplyRemoteFunction(
            RequestReplyFunctionBuilder.requestReplyFunctionBuilder(
                    REMOTE_GREET, URI.create("http://localhost:5000/statefun"))
                .withPersistedState("seen_count")
                .withMaxRequestDuration(Duration.ofSeconds(15))
                .withMaxNumBatchRequests(500))
        .withEgressId(GREETINGS)
        .build(env);
```

As you can see, instead of binding functions, ingresses, and egresses through modules as you would with a typical Stateful
Functions application, you bind them directly to the ``DataStream`` job using a ``StatefulFunctionDataStreamBuilder``:

* Remote functions are bound using the `withRequestReplyRemoteFunction` method. [Specification of the remote function]({{< ref "docs/modules/http-endpoint" >}})
such as service endpoint and various connection configurations can be set using the provided ``RequestReplyFunctionBuilder``.
* [Embedded functions](#embedded-functions) are bound using ``withFunctionProvider``.
* Egress identifiers used by functions need to be bound with the `withEgressId` method.

### Consuming a DataStream Egress

Finally, you can obtain an egress as a ``DataStream`` from the result ``StatefulFunctionEgressStreams``:

```java
EgressIdentifier<String> GREETINGS = new EgressIdentifier<>("example", "greetings", String.class);

StatefulFunctionEgressStreams egresses = ...

DataStream<String> greetingsEgress = egresses.getDataStreamForEgressId(GREETINGS);
```

The obtained egress ``DataStream`` can be further processed as in a typical Flink streaming application.

## Configuration

Like a typical Stateful Functions application, configuration specific to Stateful Functions is set through the ``flink-conf.yaml`` file, as explained [here]({{< ref "docs/deployment/configurations" >}}).
You can also overwrite the base settings for each individual job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
statefunConfig.setGlobalConfiguration("someGlobalConfig", "foobar");
statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

StatefulFunctionEgressStreams egresses =
    StatefulFunctionDataStreamBuilder.builder("example")
        ...
        .withConfiguration(statefunConfig)
        .build(env);
```

{{< hint info >}}
The setFlinkJobName method on StatefulFunctionsConfig does not have effect using this SDK.
You need to define the job name as you normally would via Flink's DataStream API.
{{< /hint >}}

## Embedded Functions

Along with invoking a remote functions, the `StatefulFunctionDataStreamBuilder` supports a special type of function; the embedded function. Embedded functions run directly within the Flink runtime and have the same
deployment and operational characteristics of Apache Flink's other operators. In practice, this means they will have the highest throughput - functions are invoked directly instead of through RPC - but they cannot be deployed or scaled without downtime and can effect the performance and stability of the entire cluster. 

{{< hint info >}}
For most applications the community encourages the use of the standard SDKs which run functions
remotely from the Apache Flink runtime.
You can find more information on the standard remote Java SDK [here]({{< ref "docs/sdk/java" >}}).
{{< /hint >}}

### Defining An Embedded Stateful Function

An embedded stateful function is any class that implements the StatefulFunction interface. The following is an example of a simple hello world function.

```java
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class FnHelloWorld implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
        System.out.println("Hello " + input.toString());
    }
}
```

Functions process each incoming message through their ``invoke`` method.
Input's are untyped and passed through the system as a ``java.lang.Object`` so one function can potentially process multiple types of messages.

The ``Context`` provides metadata about the current message and function, and is how you can call other functions or external systems.
Functions are invoked based on a function type and unique identifier.

### Function Types and Messaging

In Java, function types are defined as logical pointers composed of a namespace and name.
The type is bound to the implementing class when registered.
Below is an example function type for the hello world function.

```java
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.FunctionType;

/** A function type that will be bound to {@link FnHelloWorld}. */
public class Identifiers {
    public static final FunctionType HELLO_TYPE = new FunctionType("apache/flink", "hello");
}
```

This type can then be referenced from other functions to create an address and message a particular instance.

```java
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/** A simple stateful function that sends a message to the user with id "user1" */
public class FnCaller implements StatefulFunction {

  @Override
  public void invoke(Context context, Object input) {
    context.send(Identifiers.HELLO_TYPE, "user1", new MyUserMessage());
  }
}
```

### Sending Delayed Messages

Functions are able to send messages on a delay so that they will arrive after some duration.
Functions may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking so functions will continue to process records between the time a delayed message is sent and received.

```java
package org.apache.flink.statefun.docs.delay;

import java.time.Duration;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class FnDelayedMessage implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
        if (input instanceof Message) {
            System.out.println("Hello");
            context.sendAfter(Duration.ofMinutes(1), context.self(), new DelayedMessage());
        }

        if (input instanceof DelayedMessage) {
            System.out.println("Welcome to the future!");
        }
    }
}
```

### Completing Async Requests

When interacting with external systems, such as a database or API, one needs to take care that communication delay with the external system does not dominate the application’s total work.
Stateful Functions allows registering a Java ``CompletableFuture`` that will resolve to a value at some point in the future.
Future's are registered along with a metadata object that provides additional context about the caller.

When the future completes, either successfully or exceptionally, the caller function type and id will be invoked with a ``AsyncOperationResult``.
An asynchronous result can complete in one of three states:

* Success: The asynchronous operation has succeeded, and the produced result can be obtained via ``AsyncOperationResult#value``.
* Failure: The asynchronous operation has failed, and the cause can be obtained via ``AsyncOperationResult#throwable``.
* Unknown: The stateful function was restarted, possibly on a different machine, before the ``CompletableFuture`` was completed, therefore it is unknown what is the status of the asynchronous operation.

```java
package org.apache.flink.statefun.docs.async;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

@SuppressWarnings("unchecked")
public class EnrichmentFunction implements StatefulFunction {

    private final QueryService client;

    public EnrichmentFunction(QueryService client) {
        this.client = client;
    }

    @Override
    public void invoke(Context context, Object input) {
	    if (input instanceof User) {
            onUser(context, (User) input);
        } else if (input instanceof AsyncOperationResult) {
            onAsyncResult((AsyncOperationResult) input);
        }
    }

    private void onUser(Context context, User user) {
        CompletableFuture<UserEnrichment> future = client.getDataAsync(user.getUserId());
        context.registerAsyncOperation(user, future);
    }

    private void onAsyncResult(AsyncOperationResult<User, UserEnrichment> result) {
        if (result.successful()) {
            User metadata = result.metadata();
            UserEnrichment value = result.value();
            System.out.println(
                String.format("Successfully completed future: %s %s", metadata, value));
        } else if (result.failure()) {
            System.out.println(
                String.format("Something has gone terribly wrong %s", result.throwable()));
        } else {
            System.out.println("Not sure what happened, maybe retry");
        }
    }
}
```

### Persistence

Stateful Functions treats state as a first class citizen and so all stateful functions can easily define state that is automatically made fault tolerant by the runtime.
All stateful functions may contain state by merely defining one or more persisted fields.

The simplest way to get started is with a ``PersistedValue``, which is defined by its name and the class of the type that it stores.
The data is always scoped to a specific function type and identifier.
Below is a stateful function that greets users based on the number of times they have been seen.

{{< hint info >}}
All **PersistedValue**, **PersistedTable**, and **PersistedAppendingBuffer** fields must be marked with a **@Persisted** annotation or they will not be made fault tolerant by the runtime.
{{< /hint >}}

```java
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class FnUserGreeter implements StatefulFunction {

	public static FunctionType TYPE = new FunctionType("example", "greeter");

	@Persisted
	private final PersistedValue<Integer> count = PersistedValue.of("count", Integer.class);

	public void invoke(Context context, Object input) {
		String userId = context.self().id();
		int seen = count.getOrDefault(0);

		switch (seen) {
			case 0:
				System.out.println(String.format("Hello %s!", userId));
				break;
			case 1:
				System.out.println("Hello Again!");
				break;
			case 2:
				System.out.println("Third time is the charm :)");
				break;
			default:
				System.out.println(String.format("Hello for the %d-th time", seen + 1));
		}

		count.set(seen + 1);
	}
}
```

``PersistedValue`` comes with the right primitive methods to build powerful stateful applications.
Calling ``PersistedValue#get`` will return the current value of an object stored in state, or ``null`` if nothing is set.
Conversely, ``PersistedValue#set`` will update the value in state and ``PersistedValue#clear`` will delete the value from state.

#### Collection Types

Along with ``PersistedValue``, the Java SDK supports two persisted collection types.
``PersistedTable`` is a collection of keys and values, and ``PersistedAppendingBuffer`` is an append-only buffer.

These types are functionally equivalent to ``PersistedValue<Map>`` and ``PersistedValue<Collection>`` respectively but may provide better performance in some situations.

```java
@Persisted
PersistedTable<String, Integer> table = PersistedTable.of("my-table", String.class, Integer.class);

@Persisted
PersistedAppendingBuffer<Integer> buffer = PersistedAppendingBuffer.of("my-buffer", Integer.class);
```

#### Dynamic State Registration

Using the above state types, a function's persisted state must be defined eagerly. You cannot use those state types to
register a new persisted state during invocations (i.e., in the ``invoke`` method) or after the function instance is created.

If dynamic state registration is required, it can be achieved using a ``PersistedStateRegistry``:

```java
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class MyFunction implements StatefulFunction {

    @Persisted
    private final PersistedStateRegistry registry = new PersistedStateRegistry();

    private PersistedValue<Integer> value;

    public void invoke(Context context, Object input) {
        if (value == null) {
            value = PersistedValue.of("my-value", Integer.class);
            registry.registerValue(value);
        }
        int count = value.getOrDefault(0);
        // ...
    }
}
```

Note how the ``PersistedValue`` field doesn't need to be annotated with the ``@Persisted`` annotations, and is initially
empty. The state object is dynamically created during invocation and registered with the ``PersistedStateRegistry`` so
that the system picks it up to be managed for fault-tolerance.

#### State Expiration

Persisted states may be configured to expire and be deleted after a specified duration.
This is supported by all types of state:

```java
@Persisted
PersistedValue<Integer> value = PersistedValue.of(
    "my-value",
    Integer.class,
    Expiration.expireAfterWriting(Duration.ofHours(1)));

@Persisted
PersistedTable<String, Integer> table = PersistedTable.of(
    "my-table",
    String.class,
    Integer.class,
    Expiration.expireAfterWriting(Duration.ofMinutes(5)));

@Persisted
PersistedAppendingBuffer<Integer> buffer = PersistedAppendingBuffer.of(
    "my-buffer",
    Integer.class,
    Expiration.expireAfterWriting(Duration.ofSeconds(30)));
```

There are two expiration modes supported:

```java
Expiration.expireAfterWriting(...)

Expiration.expireAfterReadingOrWriting(...)
```

State TTL configurations are made fault-tolerant by the runtime. In the case of downtime, state entries that should have been removed during said downtime will be purged immediately on restart.

### Function Providers and Dependency Injection

Stateful functions are created across a distributed cluster of nodes.
``StatefulFunctionProvider`` is a factory class for creating a new instance of a ``StatefulFunction`` the first time it is activated.

```java
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.docs.dependency.ProductionDependency;
import org.apache.flink.statefun.docs.dependency.RuntimeDependency;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

public class CustomProvider implements StatefulFunctionProvider {

	public StatefulFunction functionOfType(FunctionType type) {
		RuntimeDependency dependency = new ProductionDependency();
		return new FnWithDependency(dependency);
	}
}
```

Providers are called once per type on each parallel worker, not for each id.
If a stateful function requires custom configurations, they can be defined inside a provider and passed to the functions' constructor.
This is also where shared physical resources, such as a database connection, can be created that are used by any number of function instances.
