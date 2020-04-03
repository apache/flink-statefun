---
title: Distributed Architecture 
nav-id: dist-arch
nav-pos: 3
nav-title: Architecture
nav-parent_id: concepts
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

A Stateful Functions deployment consists of a few components interacting together. Here we describe these pieces and their relationship to each other and the Apache Flink runtime.

* This will be replaced by the TOC
{:toc}

## High-level View

A *Stateful Functions* deployment consists of a set of **Apache Flink Stateful Functions** processes and, optionally, various deployments that execute remote functions.

<p class="text-center">
    <img width="80%" src="{{ site.baseurl }}/fig/concepts/arch_overview.svg"/>
</p>

The Flink worker processes (TaskManagers) receive the events from the ingress systems (Kafka, Kinesis, etc.) and route them to the target functions. They invoke the functions and route the resulting messages to the next respective target functions. Messages designated for egress are written to an egress system (again, Kafka, Kinesis, ...).

## Components

The heavy lifting is done by the Apache Flink processes, which manage the state, handle the messaging, and invoke the stateful functions.
The Flink cluster consists typically of one master and multiple workers (TaskManagers).

<p class="text-center">
    <img width="80%" src="{{ site.baseurl }}/fig/concepts/arch_components.svg"/>
</p>

In addition to the Apache Flink processes, a full deployment requires [ZooKeeper](https://zookeeper.apache.org/) (for [master failover](https://ci.apache.org/projects/flink/flink-docs-stable/ops/jobmanager_high_availability.html)) and bulk storage (S3, HDFS, NAS, GCS, Azure Blob Store, etc.) to store Flink's [checkpoints](https://ci.apache.org/projects/flink/flink-docs-master/concepts/stateful-stream-processing.html#checkpointing). In turn, the deployment requires no database, and Flink processes do not require persistent volumes.

## Logical Co-location, Physical Separation

A core principle of many Stream Processors is that application logic and the application state must be co-located. That approach is the basis for their out-of-the box consistency. Stateful Function takes a unique approach to that by *logically co-locating* state and compute, but allowing to *physically separate* them.

  - *Logical co-location:* Messaging, state access/updates and function invocations are managed tightly together, in the same way as in Flink's DataStream API. State is sharded by key, and messages are routed to the state by key. There is a single writer per key at a time, also scheduling the function invocations.

  - *Physical separation:* Functions can be executed remotely, with message and state access provided as part of the invocation request. This way, functions can be managed independently, like stateless processes.


## Deployment Styles for Functions

The stateful functions themselves can be deployed in various ways that trade off certain properties with each other: loose coupling and independent scaling on the one hand with performance overhead on the other hand. Each module of functions can be of a different kind, so some functions can run remote, while others could run embedded.

#### Remote Functions

*Remote Functions* use the above-mentioned principle of *physical separation* while maintaining *logical co-location*. The state/messaging tier (i.e., the Flink processes) and the function tier are deployed, managed, and scaled independently.

Function invocations happen through an HTTP / gRPC protocol and go through a service that routes invocation requests to any available endpoint, for example a Kubernetes (load-balancing) service, the AWS request gateway for Lambda, etc. Because invocations are self-contained (contain message, state, access to timers, etc.) the target functions can treated like any stateless application.

<p class="text-center">
	<img width="80%" src="{{ site.baseurl }}/fig/concepts/arch_funs_remote.svg"/>
</p>


Refer to the documentation on the [Python SDK]({{ site.baseurl }}/sdk/python.html) and [remote modules]({{ site.baseurl }}/sdk/modules.html#remote-module) for details. 

#### Co-located Functions

An alternative way of deploying functions is *co-location* with the Flink JVM processes. In such a setup, each Flink TaskManager would talk to one Function process sitting *"next to it"*. A common way to do this is to use a system like Kubernetes and deploy pods consisting of a Flink container and the function side-car container; the two communicate via the pod-local network.

This mode supports different languages while avoiding to route invocations through a Service/LoadBalancer, but it cannot scale the state and compute parts independently.

<p class="text-center">
	<img width="80%" src="{{ site.baseurl }}/fig/concepts/arch_funs_colocated.svg"/>
</p>

This style of deployment is similar to how Flink's Table API and API Beam's portability layer deploy and execute non-JVM functions.

#### Embedded Functions

*Embedded Functions* are similar to the execution mode of Stateful Functions 1.0 and to Flink's Java/Scala stream processing APIs. Functions are run in the JVM and are directly invoked with the messages and state access. This is the most performant way, though at the cost of only supporting JVM languages. Updates to functions mean updating the Flink cluster.

<p class="text-center">
	<img width="80%" src="{{ site.baseurl }}/fig/concepts/arch_funs_embedded.svg"/>
</p>

Following the database analogy, Embedded Functions are a bit like *Stored Procedures*, but in a more principled way: The Functions here are normal Java/Scala/Kotlin functions implementing standard interfaces, and can be developed/tested in any IDE.
