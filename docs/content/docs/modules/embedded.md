---
title: 'Embedded Modules'
weight: 5
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

# Embedded Module Configuration

Embedded modules allow users to load code into the Stateful Functions runtime that is executed directly within the cluster.
This is usually to allow plugging in [custom ingress and egress implementations]({{< ref "docs/modules/io/flink-connectors">}}).
Additionally, and embedded module may include [embedded functions]({{< ref "docs/sdk/flink-datastream#embedded-functions" >}}) that run within the cluster. 

Embedded modules should be used with care, they cannot be deployed or scaled without downtime and can effect the performance and stability of the entire cluster.

{{< hint info >}}
If your application is comprised mostly of embedded elements, the community encourages the use of Stateful Functions [DataStream Interop]({{< ref "docs/sdk/flink-datastream" >}}). 
{{< /hint >}}

To get started, add the embedded Java SDK as a dependency to your application.

{{< artifact statefun-sdk-embedded >}}

## Defining an Embedded Module

This module type only supports JVM-based languages and is defined by implementing the `StatefulFunctionModule` interface. Embedded modules offer a single configuration method where stateful functions bind to the system based on their function type. Runtime configurations are available through the globalConfiguration, which is the union of all configurations in the applications `flink-conf.yaml` under the prefix `statefun.module.global-config`, and any command line arguments passed in the form --key value.

```java
package org.apache.flink.statefun.docs;

import java.util.Map;
import org.apche.flink.statefun.sdk.spi.StatefulFunctionModule;

public class EmbeddedModule implements StatefulFunctionModule {
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        // Embedded functions, ingresses, routers, and egresses
        // can be bound to the Binder
    }
}
```

Embedded modules leverage [Java’s Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) for discovery. This means that every JAR should contain a file org.apache.flink.statefun.sdk.spi.StatefulFunctionModule in the `META_INF/services` resource directory that lists all available modules that it provides.

```
org.apache.flink.statefun.docs.EmbeddedModule
```

## Deployment 

Embedded modules should be packaged as a fat-jar, containing all required dependencies and added to the StateFun runtime image. 

```dockerfile
FROM apache/flink-statefun:{{< version >}}

RUN mkdir -p /opt/statefun/modules/my-embedded
COPY embedded.jar /opt/statefun/modules/my-embedded/embedded.jar
```