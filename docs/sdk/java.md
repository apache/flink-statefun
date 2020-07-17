---
title: Java SDK 
nav-id: java-sdk
nav-pos: 1
nav-title: Java
nav-parent_id: sdk
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

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.
The Java SDK is supported as an [embedded module]({{ site.baseurl }}/sdk/modules.html#embedded-module).

To get started, add the Java SDK as a dependency to your application.

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>statefun-sdk</artifactId>
	<version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

* This will be replaced by the TOC
{:toc}

## Defining A Stateful Function

A stateful function is any class that implements the ``StatefulFunction`` interface.
The following is an example of a simple hello world function.

{% highlight java %}
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class FnHelloWorld implements StatefulFunction {

	@Override
	public void invoke(Context context, Object input) {
		System.out.println("Hello " + input.toString());
	}
}
{% endhighlight %}

Functions process each incoming message through their ``invoke`` method.
Input's are untyped and passed through the system as a ``java.lang.Object`` so one function can potentially process multiple types of messages.

The ``Context`` provides metadata about the current message and function, and is how you can call other functions or external systems.
Functions are invoked based on a function type and unique identifier.

### Stateful Match Function 

Stateful functions provide a powerful abstraction for working with events and state, allowing developers to build components that can react to any kind of message.
Commonly, functions only need to handle a known set of message types, and the ``StatefulMatchFunction`` interface provides an opinionated solution to that problem.

#### Simple Match Function

Stateful match functions are an opinionated variant of stateful functions for precisely this pattern.
Developers outline expected types, optional predicates, and well-typed business logic and let the system dispatch each input to the correct action.
Variants are bound inside a ``configure`` method that is executed once the first time an instance is loaded.

{% highlight java %}
package org.apache.flink.statefun.docs.match;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;

public class FnMatchGreeter extends StatefulMatchFunction {

	@Override
	public void configure(MatchBinder binder) {
		binder
			.predicate(Customer.class, this::greetCustomer)
			.predicate(Employee.class, Employee::isManager, this::greetManager)
			.predicate(Employee.class, this::greetEmployee);
	}

	private void greetCustomer(Context context, Customer message) {
		System.out.println("Hello customer " + message.getName());
	}

	private void greetEmployee(Context context, Employee message) {
		System.out.println("Hello employee " + message.getEmployeeId());
	}

	private void greetManager(Context context, Employee message) {
		System.out.println("Hello manager " + message.getEmployeeId());
	}
}
{% endhighlight %}

#### Making Your Function Complete

Similar to the first example, match functions are partial by default and will throw an ``IllegalStateException`` on any input that does not match any branch.
They can be made complete by providing an ``otherwise`` clause that serves as a catch-all for unmatched input, think of it as a default clause in a Java switch statement.
The ``otherwise`` action takes its message as an untyped ``java.lang.Object``, allowing you to handle any unexpected messages.

{% highlight java %}
package org.apache.flink.statefun.docs.match;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;

public class FnMatchGreeterWithCatchAll extends StatefulMatchFunction {

	@Override
	public void configure(MatchBinder binder) {
		binder
			.predicate(Customer.class, this::greetCustomer)
			.predicate(Employee.class, Employee::isManager, this::greetManager)
			.predicate(Employee.class, this::greetEmployee)
			.otherwise(this::catchAll);
	}

	private void greetCustomer(Context context, Customer message) {
		System.out.println("Hello customer " + message.getName());
	}

	private void greetEmployee(Context context, Employee message) {
		System.out.println("Hello employee " + message.getEmployeeId());
	}

	private void greetManager(Context context, Employee message) {
		System.out.println("Hello manager " + message.getEmployeeId());
	}

	private void catchAll(Context context, Object message) {
		System.out.println("Hello unexpected message");
	}
}
{% endhighlight %}

#### Action Resolution Order

Match functions will always match actions from most to least specific using the following resolution rules.

First, find an action that matches the type and predicate. If two predicates will return true for a particular input, the one registered in the binder first wins.
Next, search for an action that matches the type but does not have an associated predicate.
Finally, if a catch-all exists, it will be executed or an ``IllegalStateException`` will be thrown.

## Function Types and Messaging

In Java, function types are defined as logical pointers composed of a namespace and name.
The type is bound to the implementing class in the [module]({{ site.baseurl }}/sdk/modules.html#embedded-module) definition.
Below is an example function type for the hello world function.

{% highlight java %}
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.FunctionType;

/** A function type that will be bound to {@link FnHelloWorld}. */
public class Identifiers {

  public static final FunctionType HELLO_TYPE = new FunctionType("apache/flink", "hello");
}
{% endhighlight %}

This type can then be referenced from other functions to create an address and message a particular instance.

{% highlight java %}
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
{% endhighlight %}

## Sending Delayed Messages

Functions are able to send messages on a delay so that they will arrive after some duration.
Functions may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking so functions will continue to process records between the time a delayed message is sent and received.

{% highlight java %}
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
{% endhighlight %}

## Completing Async Requests

When interacting with external systems, such as a database or API, one needs to take care that communication delay with the external system does not dominate the application’s total work.
Stateful Functions allows registering a Java ``CompletableFuture`` that will resolve to a value at some point in the future.
Future's are registered along with a metadata object that provides additional context about the caller.

When the future completes, either successfully or exceptionally, the caller function type and id will be invoked with a ``AsyncOperationResult``.
An asynchronous result can complete in one of three states:

### Success

The asynchronous operation has succeeded, and the produced result can be obtained via ``AsyncOperationResult#value``.

### Failure

The asynchronous operation has failed, and the cause can be obtained via ``AsyncOperationResult#throwable``.

### Unknown

The stateful function was restarted, possibly on a different machine, before the ``CompletableFuture`` was completed, therefore it is unknown what is the status of the asynchronous operation.

{% highlight java %}
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
{% endhighlight %}

## Persistence

Stateful Functions treats state as a first class citizen and so all stateful functions can easily define state that is automatically made fault tolerant by the runtime.
All stateful functions may contain state by merely defining one or more persisted fields.

The simplest way to get started is with a ``PersistedValue``, which is defined by its name and the class of the type that it stores.
The data is always scoped to a specific function type and identifier.
Below is a stateful function that greets users based on the number of times they have been seen.

<div class="alert alert-info">
  <strong>Attention:</strong> All <b>PersistedValue</b>, <b>PersistedTable</b>, and <b>PersistedAppendingBuffer</b> fields must be marked with a <b>@Persisted</b> annotation or they will not be made fault tolerant by the runtime.
</div>

{% highlight java %}
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
{% endhighlight %}

``PersistedValue`` comes with the right primitive methods to build powerful stateful applications.
Calling ``PersistedValue#get`` will return the current value of an object stored in state, or ``null`` if nothing is set.
Conversely, ``PersistedValue#set`` will update the value in state and ``PersistedValue#clear`` will delete the value from state.

### Collection Types

Along with ``PersistedValue``, the Java SDK supports two persisted collection types.
``PersistedTable`` is a collection of keys and values, and ``PersistedAppendingBuffer`` is an append-only buffer.

These types are functionally equivalent to ``PersistedValue<Map>`` and ``PersistedValue<Collection>`` respectively but may provide better performance in some situations.

{% highlight java %}
@Persisted
PersistedTable<String, Integer> table = PersistedTable.of("my-table", String.class, Integer.class);

@Persisted
PersistedAppendingBuffer<Integer> buffer = PersistedAppendingBuffer.of("my-buffer", Integer.class);
{% endhighlight %}

### State Expiration

Persisted states may be configured to expire and be deleted after a specified duration.
This is supported by all types of state:

{% highlight java %}
@Persisted
PersistedValue<Integer> table = PersistedValue.of(
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
{% endhighlight %}

There are two expiration modes supported:

{% highlight java %}
Expiration.expireAfterWriting(...)

Expiration.expireAfterReadingOrWriting(...)
{% endhighlight %}

State TTL configurations are made fault-tolerant by the runtime. In the case of downtime, state entries that should have been removed during said downtime will be purged immediately on restart.

## Function Providers and Dependency Injection

Stateful functions are created across a distributed cluster of nodes.
``StatefulFunctionProvider`` is a factory class for creating a new instance of a ``StatefulFunction`` the first time it is activated.

{% highlight java %}
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
{% endhighlight %}

Providers are called once per type on each parallel worker, not for each id.
If a stateful function requires custom configurations, they can be defined inside a provider and passed to the functions' constructor.
This is also where shared physical resources, such as a database connection, can be created that are used by any number of virtual functions.
Now, tests can quickly provide mock, or test dependencies, without the need for complex dependency injection frameworks.

{% highlight java %}
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.docs.dependency.RuntimeDependency;
import org.apache.flink.statefun.docs.dependency.TestDependency;
import org.junit.Assert;
import org.junit.Test;

public class FunctionTest {

	@Test
	public void testFunctionWithCustomDependency() {
		RuntimeDependency dependency = new TestDependency();
		FnWithDependency function = new FnWithDependency(dependency);

		Assert.assertEquals("It appears math is broken", 1 + 1, 2);
	}
}
{% endhighlight %}
