---
title: Flink Connectors
weight: 4
type: docs
aliases:
  - /io-module/flink-connectors.html
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

# Flink Connectors


The source-sink I/O module allows you to plug in existing, or custom, Flink connectors that are not already integrated into a dedicated I/O module.
Please see the official Apache Flink documentation for a full list of [available connectors](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/) as well as details on how to build your own.
Connectors can be plugged into the runtime via an [embedded module]({{< ref "docs/deployment/embedded" >}})

## Dependency

To use a custom Flink connector, please include the following dependency in your pom.

{{< artifact statefun-flink-io >}}

## Source Spec

A source function spec creates an ingress from a Flink source function that outputs a `TypedValue`.
Additionally, each ingress requires a `Router` function that takes each incoming message and routes it to one or more function instances. 

```java
package org.apache.flink.statefun.docs.io.flink;

import java.util.Map;
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class ModuleWithSourceSpec implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        IngressIdentifier<TypedValue> id =
            new IngressIdentifier<>(TypedValue.class, "com.example", "custom-source");
        IngressSpec<TypedValue> spec = new SourceFunctionSpec<>(id, new FlinkSource<>());
        binder.bindIngress(spec);
        binder.bindIngressRouter(id, new CustomRouter());
    }
}
```


## Sink Spec

A sink function spec creates an egress from a Flink sink function that consumes a `TypedValue`.

```java
package org.apache.flink.statefun.docs.io.flink;

import java.util.Map;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.flink.io.datastream.SinkFunctionSpec;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class ModuleWithSinkSpec implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        EgressIdentifier<TypedValue> id = new EgressIdentifier<>("com.example", "custom-sink", TypedValue.class);
        EgressSpec<TypedValue> spec = new SinkFunctionSpec<>(id, new FlinkSink<>());
        binder.bindEgress(spec);
    }
}
```

Functions then send messages to the egress using a generic egress message builder.

{{< tabs "egress" >}}
{{< tab "Python" >}}
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

    context.send_egress(egress_message_builder(
        typename='com.example/custom-sink',
        value=f"Hello {name} for the {seen}th time!",
        value_type=StringType))
```
{{< /tab >}}
{{< tab "Java" >}}
```java
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;

public class GreeterFn implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/greeter");

    static final TypeName CUSTOM_EGRESS = TypeName.typeNameFromString("com.example/custom-sink");

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
            EgressMessageBuilder.forEgress(CUSTOM_EGRESS)
                .withUtf8Value("Hello " + name + " for the " + seen + "th time!")
                .build());

        return context.done();
    }
}
```
{{< /tab >}}
{{< /tabs >}}