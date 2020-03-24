---
title: Application Building Blocks 
nav-id: building-blocks
nav-pos: 1
nav-title: Application Building Blocks
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

Stateful Functions provides a framework for building event drivent applications. Here, we explain important aspects of Stateful Function’s architecture.

* This will be replaced by the TOC
{:toc}

## Event Ingress

Stateful Function applications sit squarely in the event driven space, so the natural place to start is with getting events into the system.

<p class="text-center">
    <img width="80%" src="{{ site.baseurl }}/fig/concepts/statefun-app-ingress.svg"/>
</p>

In stateful functions, the component that ingests records into the system is called an event ingress.
This can be anything from a Kafka topic, to a messsage queue, to an http request - anything that can get data into the system and trigger the intitial functions to begin computation.

## Stateful Functions

At the core of the diagram are the namesake stateful functions.

<p class="text-center">
	<img width="80%" src="{{ site.baseurl }}/fig/concepts/statefun-app-functions.svg"/>
</p>

Think of these as the building blocks for your service.
They can message each other arbitrarily, which is one way in which this framework moves away from the traditional stream processing view of the world.
Instead of building up a static dataflow DAG, these functions can communicate with each other in arbitrary, potentially cyclic, even round trip ways.

If you are familiar with actor programming, this does share certain similarities in its ability to dynamically message between components.
However, there are a number of significant differences.

## Persisted States

The first is that all functions have locally embedded state, known as persisted states.

<p class="text-center">
	<img width="80%" src="{{ site.baseurl }}/fig/concepts/statefun-app-state.svg"/>
</p>

One of Apache Flink's core strengths is its ability to provide fault-tolerant local state.
When inside a function, while it is performing some computation, you are always working with local state in local variables.

## Fault Tolerance

For both state and messaging, Stateful Function's is still able to provide the exactly-once guarantees users expect from a modern data processessing framework.

<p class="text-center">
    <img width="80%" src="{{ site.baseurl }}/fig/concepts/statefun-app-fault-tolerance.svg"/>
</p>

In the case of failure, the entire state of the world (both persisted states and messages) are rolled back to simulate completely failure free execution.

These guarantees are provided with no database required, instead Stateful Function's leverages Apache Flink's proven snapshotting mechanism.

## Event Egress

Finally, applications can output data to external systems via event egress's.

<p class="text-center">
    <img width="80%" src="{{ site.baseurl }}/fig/concepts/statefun-app-egress.svg"/>
</p>

Of course, functions perform arbitrary computation and can do whatever they like, which includes making RPC calls and connecting to other systems.
By using an event egress, applications can leverage pre-built integrations built on-top of the Apache Flink connector ecosystem.
