---
title: Apache Flink Stateful Functions 
type: docs
bookToc: false
aliases:
  - /sdk/
  - /docs/sdk/overview/
  - /sdk/external.html
  - /docs/sdk/external/
  - /getting-started/project-setup.html
  - /docs/getting-started/project-setup/
  - /getting-started/python_walkthrough.html
  - /docs/getting-started/python_walkthrough/
  - /getting-started/java_walkthrough.html
  - /docs/getting-started/java_walkthrough/
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

<div style="text-align: center">
  <h1>
    Stateful Functions: A Platform-Independent Stateful Serverless Stack
  </h1>

  <h4 style="color: #696969">A simple way to create efficient, scalable, and consistent applications on modern infrastructure - at small and large scale.</h4>
</div>

Stateful Functions is an API that simplifies the building of **distributed stateful applications** with a runtime built for **serverless architectures**. It brings together the benefits of stateful stream processing - the processing of large datasets with low latency and bounded resource constraints - along with a runtime for modeling stateful entities that supports location transparency, concurrency, scaling, and resiliency.

{{< img src="/fig/concepts/arch_overview.svg" alt="Stateful Functions" width="50%" >}}

It is designed to work with modern architectures, like cloud-native deployments and popular event-driven FaaS platforms like AWS Lambda and KNative, and to provide out-of-the-box consistent state and messaging while preserving the serverless experience and elasticity of these platforms.

{{< columns >}}

### Try StateFun

If you’re interested in playing around with Stateful Functions, check out our code [playground](https://github.com/apache/flink-statefun-playground).
It provides a step by step introduction to the APIs and guides you through real applications.

### Get Help with StateFun

If you get stuck, check out our [community support resources](https://flink.apache.org/community.html). In particular, Apache Flink’s user mailing list is consistently ranked as one of the most active of any Apache project, and is a great way to get help quickly.

<--->

### Explore StateFun

The reference documentation covers all the details. Some starting points:

* [Python API]({{< ref "docs/sdk/python" >}})
* [Java API]({{< ref "docs/sdk/java" >}})
* [Golang API]({{< ref "docs/sdk/golang" >}})

### Deploy StateFun

Before putting your Stateful Functions application into production, read the [deployment overview]({{< ref "docs/deployment/overview" >}}) for details on how to successfully run and manage your system.

{{< /columns >}}

Stateful Functions is developed under the umbrella of [Apache Flink](flink.apache.org)

