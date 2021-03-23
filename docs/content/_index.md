---
title: Apache Flink Stateful Functions 
type: docs
bookToc: false
aliases:
  - /sdk/
  - /docs/sdk/overview/
  - /sdk/external.html
  - /docs/sdk/external/
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

# Stateful Functions Documentation

Stateful Functions is an API that simplifies the building of **distributed stateful applications** with a runtime built for **serverless architectures**. It brings together the benefits of stateful stream processing - the processing of large datasets with low latency and bounded resource constraints - along with a runtime for modeling stateful entities that supports location transparency, concurrency, scaling, and resiliency.

{{< img src="/fig/concepts/arch_overview.svg" alt="Stateful Functions" width="50%" >}}

It is designed to work with modern architectures, like cloud-native deployments and popular event-driven FaaS platforms like AWS Lambda and KNative, and to provide out-of-the-box consistent state and messaging while preserving the serverless experience and elasticity of these platforms.

Stateful Functions is developed under the umbrella of [Apache Flink](flink.apache.org)

## Learn By Doing

If you prefer to learn by doing, start with our code [walkthrough]({{< ref "docs/getting-started/python_walkthrough" >}}). 
It provides a step by step introduction to the API and guides you through real applications.

## Learn Concepts Step By Step

If you prefer to learn concepts step by step, start with our guide to main [concepts]({{< ref "docs/concepts/application-building-blocks" >}}).
It will walk you through all the API’s and concepts to build advanced stateful systems.

## Start A New Project 

The [project setup]({{< ref "docs/getting-started/project-setup" >}}) instructions show you how to create a project for a new Stateful Functions application in just a few steps.

