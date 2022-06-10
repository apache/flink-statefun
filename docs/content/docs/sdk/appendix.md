---
title: Appendix
weight: 10001
type: docs
aliases:
  - /sdk/python.html
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

# SDK Appendix

Stateful Functions can be implemented in any number of programming languages using different language SDKs, both official SDKs provided by the project and 3rd party SDKs maintained by community members. While certain aspects of each SDK API are tailored to feel idomatic to that language a number of basic concepts and traits are universal. This page documents common terms and concepts that exist across all languages.

## Address

Function instances are uniquly identified with an `Address` which is composed of a `TypeName` and identifier. The type name is similiar to a class in an object-oreiented language; it declares what sort of function the address references. The identifier is a primary key, which scopes the funciton call to a specific instance of the function type. When a function is invoked, all actions - including reads and writes of state values - are scoped to the current address. 

## State

While some operations simply look at one individual _message_ at a time (for example a parser), many operations remember information across multiple messages. These operations are called **stateful**. 

Some examples of stateful operations:
* When an application searches for certain event patterns, the state will store the sequence of events encountered so far.
* When aggregating events per minute/hour/day, the state holds the pending aggregates.
* When scoring machine learning model, the state holds the current version of the model parameters and information to build feature vectors
* When historic data needs to be managed, the state allows efficient access to events that occurred in the past.

Stateful Functions maintain any number of state values - of various [types](#types) - and the runtime guaruntees fault-tolerance, reliability, and scalability. State is always scoped to the current address, so multiple function instances of the same type maintain independent values. For example, a greeter type that maintains a _seen count_ will have a unique value for each user.

## Types

Stateful Function maintains its own type system to standardize serialization formats across languages and remote execution. Each SDK includes a set of predefined primitive types along with the ability to plug in custom types with your own serializers. 

| Type | Type Name | Description |
|------|-----------|-------------|
| Boolean | io.statefun.types/bool | A standard boolean type with possible values `true` and `false` |
| Integer | io.statefun.types/int | A signed 4 byte integer |
| Long | io.statefun.types/long | A signed 8 byte integer |
| Float | io.statefun.types/float | A signed 4 byte floating point number |
| Double | io.statefun.types/double | A signed 8 byte floating point number |
| String | io.statefun.types/string | A UTF-8 encoded string |

