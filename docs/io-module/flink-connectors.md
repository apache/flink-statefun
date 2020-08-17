---
title: Flink Connectors
nav-id: flink-connectors
nav-pos: 3
nav-title: Flink Connectors
nav-parent_id: io-module
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


The source-sink I/O module allows you to plug in existing, or custom, Flink connectors that are not already integrated into a dedicated I/O module.
For details of how to build a custom connector see the official [Apache Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable).

* This will be replaced by the TOC
{:toc}

## Dependency

To use a custom Flink connector, please include the following dependency in your pom.

{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>statefun-flink-io</artifactId>
    <version>{{ site.version }}</version>
    <scope>provided</scope>
</dependency>
{% endhighlight %}

## Source Spec

A source function spec creates an ingress from a Flink source function.

{% highlight java %}
package org.apache.flink.statefun.docs.io.flink;

import java.util.Map;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class ModuleWithSourceSpec implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        IngressIdentifier<User> id = new IngressIdentifier<>(User.class, "example", "users");
        IngressSpec<User> spec = new SourceFunctionSpec<>(id, new FlinkSource<>());
        binder.bindIngress(spec);
    }
}
{% endhighlight %}

## Sink Spec

A sink function spec creates an egress from a Flink sink function.

{% highlight java %}
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
        EgressIdentifier<User> id = new EgressIdentifier<>("example", "user", User.class);
        EgressSpec<User> spec = new SinkFunctionSpec<>(id, new FlinkSink<>());
        binder.bindEgress(spec);
    }
}
{% endhighlight %}
