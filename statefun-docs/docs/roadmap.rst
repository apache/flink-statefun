.. Licensed to the Apache Software Foundation (ASF) under one
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

.. _roadmap:

#######################
Roadmap and Future Work
#######################

.. contents:: :local:

Project Community
=================

We plan to contribute the Stateful Functions project to the Apache Flink project, to continue to develop it in an open-source and open-community way. We believe that developing this is the best way to grow the project, with an open community of users and contributors.

The Apache Flink community is a natural fit, because Stateful Functions currently builds on top of Apache Flink, and because the problems it solves are connected to event-driven applications and stream processing.

However, the decision whether to accept this project as a contribution ultimately rests with the Flink community. We are in touch with the Flink community, stay tuned for updates.

Features
========

The project is a work-in-progress. We believe we are off to a promising direction, but there is still a way to go to make all parts of this vision a reality. There are many possible ways to enhance **Stateful Functions** for different types of applications. Possibilities for enhancements to the runtime and operations will also evolve with the evolution of capabilities of Apache Flink. The following features are on our short-term Roadmap:

Non-JVM languages / Polyglot Functions
######################################

We plan to support other languages as well, like Python, or Go. The exact design is still open, but current ideas center around running functions in separate containers next to the JVM. See `this <https://cwiki.apache.org/confluence/display/FLINK/FLIP-38%3A+Python+Table+API>`_ design proposal describing how Apache Flink and Apache Beam approach that problem.

Stronger-typed APIs
###################

As is, the stateful functions abstraction accepts untyped objects as messages. We plan to add a stronger-typed API and tools to more easily implement complex behaviors and interaction protocols between multiple functions.

Time-to-live (TTL) for Functions
################################

Logical function instances and their state live until they are explicitly dropped (i.e. cleared). TTL would support automatically dropping function instances that have not been messaged for a defined amount of time. Apache Flink’s state TTL feature can back this efficiently, by piggybacking on the RocksDB compaction mechanism to delete expired functions in the background.

Dynamic loading and unloading of Functions
##########################################

Dynamic loading of functions will support adding new functions and routers to a running cluster without disruption. This can form the backbone for update strategies or for users to build a “StatefulFunctions-as-a-Service”-style abstraction on top of this project.
