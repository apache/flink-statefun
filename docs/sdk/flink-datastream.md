---
title: "SDK for Flink DataStream Integration"
nav-id: flink-datastream-sdk
nav-pos: 1001
nav-title: "Flink DataStream"
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

This SDK may be used if you want your Stateful Functions application to consume events from, or output events to
Flink [DataStreams](https://ci.apache.org/projects/flink/flink-docs-master/dev/datastream_api.html). Using this SDK,
you may combine pipelines written with the Flink DataStream API or higher-level libraries (such as [Table API](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/),
[CEP](https://ci.apache.org/projects/flink/flink-docs-master/dev/libs/cep.html#flinkcep-complex-event-processing-for-flink) etc.,
basically anything that produces a ``DataStream``) with the programming constructs provided by Stateful Functions to build
complex streaming applications.

To use this, add the Flink DataStream Integration SDK as a dependency to your application:

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>statefun-flink-datastream</artifactId>
	<version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

* This will be replaced by the TOC
{:toc}

## SDK Overview

The following sections covers the important parts on getting started with the SDK. For the full code and working
example, please take a look at this [example](https://github.com/apache/flink-statefun/blob/master/statefun-examples/statefun-flink-datastream-example/src/main/java/org/apache/flink/statefun/examples/datastream/Example.java).

### Preparing a DataStream Ingress

All ``DataStream`` used as ingresses must contain stream elements of type ``RoutableMessage``. Each ``RoutableMessage``
carries information about the target function's address alongside the input event payload.

You can use the ``RoutableMessageBuilder`` to transform your ``DataStream``s:

{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> names = env.addSource(...)

DataStream<RoutableMessage> namesIngress =
    names.map(name ->
        RoutableMessageBuilder.builder()
            .withTargetAddress(new FunctionType("example", "greet"), name)
            .withMessageBody(name)
            .build()
    );
{% endhighlight %}

In the above example, we transformed a ``DataStream<String>`` into a ``DataStream<RoutableMessage>`` by mapping
element in the original stream to a ``RoutableMessage``, with each element targeted for the function type ``(example:greet)``.

### Binding Functions, Ingresses, and Egresses

Once you have transformed your stream ingresses, you may start binding functions to consume the stream events, as well
as DataStream egresses to produce the outputs to:

{% highlight java %}
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
        .withFunctionProvider(GREET, unused -> new MyFunction())
        .withEgressId(GREETINGS)
        .build(env);
{% endhighlight %}

As you can see, instead of binding functions, ingresses, and egresses through modules as you would with a typical Stateful
Functions application, you bind them directly to the ``DataStream`` job using a ``StatefulFunctionDataStreamBuilder``:

* Remote functions are bind using the `withRequestReplyRemoteFunction` method. [Specification of the remote function]({{ site.baseurl }}/sdk/index.html#specification)
such as service endpoint and various connection configurations can be set using the provided ``RequestReplyFunctionBuilder``.
* Embedded functions are bind using ``withFunctionProvider``.
* Egress identifiers used by functions need to be bind with the `withEgressId` method.

### Consuming a DataStream Egress

Finally, you can obtain an egress as a ``DataStream`` from the result ``StatefulFunctionEgressStreams``:

{% highlight java %}
EgressIdentifier<String> GREETINGS = new EgressIdentifier<>("example", "greetings", String.class);

StatefulFunctionEgressStreams egresses = ...

DataStream<String> greetingsEgress = egresses.getDataStreamForEgressId(GREETINGS);
{% endhighlight %}

The obtained egress ``DataStream`` can be further processed as in a typical Flink streaming application.

## Configuration

Like a typical Stateful Functions application, configuration specific to Stateful Functions is set through the ``flink-conf.yaml`` file, as explained [here]({{ site.baseurl }}/deployment-and-operations/configurations.html).
You can also overwrite the base settings for each individual job:

{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
statefunConfig.setGlobalConfiguration("someGlobalConfig", "foobar");
statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

StatefulFunctionEgressStreams egresses =
    StatefulFunctionDataStreamBuilder.builder("example")
        ...
        .withConfiguration(statefunConfig)
        .build(env);
{% endhighlight %}

<div class="alert alert-info">
  <strong>Attention:</strong> The setFlinkJobName method on StatefulFunctionsConfig does not have effect using this
  SDK. You nned to define the job name as you normally would via Flink's DataStream API.
</div>
