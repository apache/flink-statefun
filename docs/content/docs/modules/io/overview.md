---
title: 'Overview'
weight: 1
type: docs
aliases:
  - /modules/io/
permalink: /moduless/io/index.html
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

# I/O Components 

Stateful Functions' I/O components allow functions to receive and send messages to external systems.
Based on the concept of Ingress (input) and Egress (output) points, and built on top of the Apache Flink® connector ecosystem, I/O components enable functions to interact with the outside world through the style of message passing.

Commonly used I/O components are bundled into the runtime by default and can be configured directly via the applications [module configuration]({{< ref "docs/modules/overview" >}}). 
Additionally, custom connectors for other systems can be [plugged in]({{< ref "docs/modules/io/flink-connectors" >}}) to the runtime.

Remember, to use one of these connectors in an application, third-party components are usually required, e.g., servers for the data stores or message queues.
