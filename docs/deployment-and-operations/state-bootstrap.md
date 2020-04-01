---
title: State Bootstrapping
nav-id: bootstrapping
nav-pos: 4
nav-title: State Bootstrapping
nav-parent_id: deployment-and-ops
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

Often times applications require some intial state provided by historical data in a file, database, or other system.
Because state is managed by Apache Flink's snapshotting mechanism, writing the intial state into a [savepoint](https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/savepoints.html) that can be used to start the Job.
Users can bootstrap initial state for Stateful Functions applications using Flink's [state processor api](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/libs/state_processor_api.html) and a ``StatefulFunctionSavepointCreator``.

To get started with the state processor api, include the following library in your application.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>statefun-flink-state-processor</artifactId>
  <version>{{ site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-state-processor-api_{{ site.scala_version }}</artifactId>
  <version>{{ site.flink_version }}</version>
</dependency>
{% endhighlight %}

<div class="alert alert-info">
  <strong>Attention:</strong> The savepoint creator currently only supports initializing the state for Java modules.
</div>

* This will be replaced by the TOC
{:toc}

## State Bootstrap Function

A ``StateBootstrapFunction`` defines how to bootstrap state for a ``StatefulFunction`` instance with a given input.

Each bootstrap functions instance directly corresponds to a ``StatefulFunction`` type.
Likewise, each instance is uniquely identified by an address, represented by the type and id of the function being bootstrapped.
Any state that is persisted by a bootstrap functions instance will be available to the corresponding live StatefulFunction instance having the same address.

For example, consider the following state bootstrap function:

{% highlight java %}
public class MyStateBootstrapFunction implements StateBootstrapFunction {

	@Persisted
	private PersistedValue<MyState> state = PersistedValue.of("my-state", MyState.class);

	@Override
	public void bootstrap(Context context, Object input) {
		state.set(extractStateFromInput(input));
	}
 }
{% endhighlight %}

Assume that this bootstrap function was provided for function type ``MyFunctionType``, and the id of the bootstrap function instance was ``id-13``. 
The function writes persisted state of name ``my-state`` using the given bootstrap data. 
After restoring a Stateful Functions application from the savepoint generated using this bootstrap function, the stateful function instance with address ``(MyFunctionType, id-13)`` will already have state values available under state name `my-state`.

## Creating A Savepoint

Savepoints are created by defining certain metadata, such as max parallelism and state backend.
The default state backend is [RocksDB](https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/state_backends.html#the-rocksdbstatebackend).

{% highlight java %}
int maxParallelism = 128;
StatefulFunctionsSavepointCreator newSavepoint = new StatefulFunctionsSavepointCreator(maxParallelism);
{% endhighlight %}

Each input data set is registered in the savepoint creator with a [router]({{ site.baseurl }}/io-module/index.html#router) that routes each record to zero or more function instances.
You may then register any number of function types to the savepoint creator, similar to how functions are registered within a stateful functions module.
Finally, specify an output location for the resulting savepoint.

{% highlight java %}
// Read data from a file, database, or other location
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

final DataSet<Tuple2<String, Integer>> userSeenCounts = env.fromElements(
	Tuple2.of("foo", 4), Tuple2.of("bar", 3), Tuple2.of("joe", 2));

// Register the dataset with a router
newSavepoint.withBootstrapData(userSeenCounts, MyStateBootstrapFunctionRouter::new);

// Register a bootstrap function to process the records
newSavepoint.withStateBootstrapFunctionProvider(
		new FunctionType("apache", "my-function"),
		ignored -> new MyStateBootstrapFunction());

newSavepoint.write("file:///savepoint/path/");

env.execute();
{% endhighlight %}

For full details of how to use Flink's DataSet api, please check the official [documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/).

## Deployment

After creating a new savpepoint, it can be used to provide the initial state for a Stateful Functions application.

<div class="codetabs" markdown="1">
<div data-lang="Image Deployment" markdown="1">
When deploying based on an image, pass the ``-s`` command to Flink [JobMaster](https://ci.apache.org/projects/flink/flink-docs-stable/concepts/glossary.html#flink-master) image.
{% highlight yaml %}
version: "2.1"
services:
  master:
    image: my-statefun-application-image
    command: -s file:///savepoint/path
{% endhighlight %}
</div>
<div data-lang="Session Cluster" markdown="1">
When deploying to a Flink session cluster, specify the savepoint argument in the Flink CLI.
{% highlight bash %}
$ ./bin/flink run -s file:///savepoint/path stateful-functions-job.jar
{% endhighlight %}
</div>
</div>