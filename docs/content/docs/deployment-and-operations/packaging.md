---
title: Packaging For Deployment
weight: 2
type: docs
aliases:
  - /deployment-and-operations/packaging.html
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

# Packaging For Deployment

Stateful Functions applications can be packaged as either standalone applications or Flink jobs that can be submitted to a cluster.

## Images

The recommended deployment mode for Stateful Functions applications is to build a Docker image.
This way, user code does not need to package any Apache Flink components.
The provided base image allows teams to package their applications with all the necessary runtime dependencies quickly.

Below is an example Dockerfile for building a Stateful Functions image with both an [embedded module]({{ site.baseurl }}/sdk/modules.html#embedded-module) and a [remote module]({{ site.baseurl }}/sdk/modules.html#remote-module) for an application called ``statefun-example``.

```dockerfile
FROM flink-statefun:{{< version >}}

RUN mkdir -p /opt/statefun/modules/statefun-example
RUN mkdir -p /opt/statefun/modules/remote

COPY target/statefun-example*jar /opt/statefun/modules/statefun-example/
COPY module.yaml /opt/statefun/modules/remote/module.yaml
```

{{< stable >}}
{{< hint info >}}
The Flink community is currently waiting for the official Docker images to be published to Docker Hub.
In the meantime, Ververica has volunteered to make Stateful Functions' images available via their public registry: 

```docker
FROM ververica/flink-statefun:{{< version >}}
```

You can follow the status of Docker Hub contribution [here](https://github.com/docker-library/official-images/pull/7749).
{{< /hint >}}
{{< /stable >}}
{{< unstable >}}
{{< hint info >}}
The Flink community does not publish images for snapshot versions.
You can build this version locally by cloning the [repo](https://github.com/apache/flink-statefun) and following the instructions in `tools/docker/README.md`
{{< /hint >}}
{{< /unstable >}}

## Flink Jar

If you prefer to package your job to submit to an existing Flink cluster, simply include ``statefun-flink-distribution`` as a dependency to your application.

{{< artifact statefun-flink-distribution >}}

It includes all of Stateful Functions' runtime dependencies and configures the application's main entry-point.

{{< hint info >}}
**Attention:** The distribution must be bundled in your application fat JAR so that it is on Flink's [user code class loader](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/debugging_classloading.html#inverted-class-loading-and-classloader-resolution-order)
{{< /hint >}}

```bash
$ ./bin/flink run -c org.apache.flink.statefun.flink.core.StatefulFunctionsJob ./statefun-example.jar
```

The following configurations are strictly required for running StateFun application.

```yaml
classloader.parent-first-patterns.additional: org.apache.flink.statefun;org.apache.kafka;com.google.protobuf
```

