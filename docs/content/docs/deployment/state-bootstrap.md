---
title: State Bootstrapping
weight: 6
type: docs
aliases:
  - /deployment-and-operations/state-bootstrap.html
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

# State Bootstrapping

Often times applications require some intial state provided by historical data in a file, database, or other system.
Because state is managed by Apache Flink's snapshotting mechanism, for Stateful Function applications, that means
writing the intial state into a [savepoint](https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/savepoints.html) that can be used to start the job.
Users can bootstrap initial state for Stateful Functions applications using Flink's [State Processor API](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/libs/state_processor_api.html) and a ``StatefulFunctionSavepointCreator``.

{{< hint warning >}}
**Attention:** The savepoint creator currently only supports initializing the state for embedded Java functions.
{{< /hint >}}

To get started, include the following libraries in your application:

{{< artifact statefun-flink-state-processor >}}
{{< artifact flink-state-processor-api withScalaVersion >}}

## State Bootstrap Function

A ``StateBootstrapFunction`` defines how to bootstrap state for a ``StatefulFunction`` instance with a given input.

Each bootstrap functions instance directly corresponds to a ``StatefulFunction`` type.
Likewise, each instance is uniquely identified by an address, represented by the type and id of the function being bootstrapped.
Any state that is persisted by a bootstrap functions instance will be available to the corresponding live ``StatefulFunction`` instance having the same address.

For example, consider the following state bootstrap function:

```java
public class MyStateBootstrapFunction implements StateBootstrapFunction {

	@Persisted
	private PersistedValue<MyState> state = PersistedValue.of("my-state", MyState.class);

	@Override
	public void bootstrap(Context context, Object input) {
		state.set(extractStateFromInput(input));
	}
 }
```

Assume that this bootstrap function was provided for function type ``MyFunctionType``, and the id of the bootstrap function instance was ``id-13``. 
The function writes persisted state of name ``my-state`` using the given bootstrap data. 
After restoring a Stateful Functions application from the savepoint generated using this bootstrap function, the stateful function instance with address ``(MyFunctionType, id-13)`` will already have state values available under state name `my-state`.

## Creating A Savepoint

Savepoints are created by defining certain metadata, such as max parallelism and state backend.
The default state backend is [RocksDB](https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/state_backends.html#the-rocksdbstatebackend).

```java
int maxParallelism = 128;
StatefulFunctionsSavepointCreator newSavepoint = new StatefulFunctionsSavepointCreator(maxParallelism);
```

Each input data set is registered in the savepoint creator with a [router]({{ site.baseurl }}/io-module/index.html#router) that routes each record to zero or more function instances.
You may then register any number of function types to the savepoint creator, similar to how functions are registered within a stateful functions module.
Finally, specify an output location for the resulting savepoint.

```java
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
```

For full details of how to use Flink's ``DataSet`` API, please check the official [documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/).

## Deployment

After creating a new savpepoint, it can be used to provide the initial state for a Stateful Functions application.

{{< tabs "153bd8f9-f442-4796-9b55-d534005f7bdf" >}}
{{< tab "Image Deployment" >}}
When deploying based on an image, pass the ``-s`` command to the Flink [JobMaster](https://ci.apache.org/projects/flink/flink-docs-stable/docs/concepts/glossary/#flink-jobmaster) image.
```yaml
version: "2.1"
services:
  master:
    image: my-statefun-application-image
    command: -s file:///savepoint/path
```
{{< /tab >}}
{{< tab "Session Cluster" >}}
When deploying to a Flink session cluster, specify the savepoint argument in the Flink CLI.
```bash
$ ./bin/flink run -s file:///savepoint/path stateful-functions-job.jar
```
{{< /tab >}}
{{< /tabs >}}
