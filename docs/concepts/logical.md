---
title: Logical Functions 
nav-id: logical-functions
nav-pos: 2
nav-title: Logical Functions
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

Stateful Function's are allocated logically, which means the system can support an unbounded number of instances with a finite amount of resources.
Logical instances do not use CPU, memory, or threads when not actively being invoked, so there is no theoretical upper limit on the number of instances that can created.
Users are encouraged to model their applications as granularly as possible, based on what makes the most sense for their application, instead of desigining applications around resource constraints.

* This will be replaced by the TOC
{:toc}

## Function Address

In a local environment, the address of an object is the same as a reference to it.
But in a Stateful Function's application, function instances are virtual and their runtime location is not exposed to the user.
Instead, an ``Address`` is used to reference a specific stateful function in the system..

<p class="text-center">
    <img width="80%" src="{{ site.baseurl }}/fig/concepts/address.svg"/>
</p>

An address is made of two components, a ``FunctionType`` and ``ID``.
A function type is similar to a class in an object-oriented language; it declares what sort of function the address references.
The id is a primary key, which scopes the function call to a specific instance of the function type.

When a function is being invoked, all actions - including reads and writes of persisted states - are scoped to the current address.

For example, imagine a there was a Stateful Function application to track the inventory of a warehouse.
One possible implementation could include an ``Inventory`` function that tracks the number units in stock for a particular item; this would be the function type.
There would then be one logical instance of this type for each SKU the warehouse manages.
If it were clothing, there might be an instance for shirts and another for pants; "shirt" and "pant" would be two ids.
Each instance may be interacted with and messaged independently.
The application is free to create as many instances as there are types of items in inventory.

## Function Lifecycle

Logical functions are neither created nor destroyed, but always exist throughout the lifetime of an application.
When an application starts, each parallel worker of the framework will create one physical object per function type.
This object will be used to execute all logical instances of that type that are run by that particular worker.
The first time a message is sent to an address, it will be as if that instance had always existed with its persisted states being empty.

Clearing all persisted states of a type is the same as destroying it.
If an instance has no state and is not actively running, then it occupies no CPU, no threads, and no memory.

An instance with data stored in one or more of its persisted values only occupies the resources necessary to store that data.
State storage is managed by the Apache Flink runtime and stored in the configured state backend.
