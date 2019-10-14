.. Copyright 2019 Ververica GmbH.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

########
Overview
########

**Stateful Functions** is a framework for building and orchestrating distributed stateful applications at scale that aims to solve some of the most significant operational challenges for developers: consistent state management, reliable interaction between distributed services and resource management.

Key Benefits
############

.. raw:: html
   :file: benefits_grid.html

.. toctree::
   :hidden:

   stateful_functions
   consistency_model
   execution_model
   tech_space

Architecture Overview
#####################

The framework is based on :ref:`functions with persistent state <stateful_functions>` that can share a pool of resources and interact arbitrarily with strong consistency guarantees. **Stateful Functions** uses a state-of-the-art runtime built on `Apache Flink <https://flink.apache.org/>`_ for distributed coordination, communication and state management.

.. topic:: Stateful Functions API
   
 The API is based on, well, stateful functions: small snippets of functionality that encapsulate business logic, somewhat similar to `actors <https://www.brianstorti.com/the-actor-model/>`_. These functions exist as virtual instances — typically, one per entity in the application (for example, per user or stock item) — and are distributed across shards, making applications **scalable out-of-the-box**. Each function has persistent user-defined state in local variables and can message other functions (including itself!). This model makes **computing with state natural and uncomplicated**.

.. topic:: Stream Processing Runtime

 The runtime that powers **Stateful Functions** is based on stream processing with Apache Flink. State is kept in the stream processing engine, co-located with the computation, giving you fast and consistent state access. **State durability and fault tolerance** build on Flink’s robust `distributed snapshots model <https://ci.apache.org/projects/flink/flink-docs-stable/internals/stream_checkpointing.html>`_.

.. topic:: Messaging Model

 In **Stateful Functions** applications, everything is inherently strongly consistent: state modifications and messaging are integrated to create the effect of **consistent state and reliable messaging** within all interacting functions. Care about consistency needs to be taken only when interacting with the "outside world”. Event Ingresses and Egresses — optionally with transactional semantics — support interaction with the “outside world” via event streams.

.. topic:: Consistency Model

 Interactions flow between functions as event streams, in the style of message passing. Apache Flink’s snapshot-based fault tolerance model was extended to support cyclic data flow graphs while ensuring **exactly-once messaging guarantees** (yay!). As a result, you can have functions messaging each other arbitrarily, efficiently and reliably.

**Stateful Functions** splits compute and storage differently to the classical two-tier architecture: one ephemeral state/compute tier and a simple persistent blob storage tier. This approach **eliminates the need to provision additional databases, key-value stores or message brokers** and effectively offloads application state management from the shoulders of developers.

Technology Space
################

**Stateful Functions** is heavily inspired by multiple existing technologies for stateless application development and orchestration. Other than Apache Flink, also Function-as-a-Service (FaaS) systems such as AWS Lambda and the `virtual stateful actor model <https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf>`_ from Microsoft Orleans served as inspiration for this project.

The framework is mostly implemented in Java and runs on the JVM. Extending the API to be cross-language compatible and support languages like Python, Go or NodeJS is part of the :ref:`Roadmap <roadmap>`. 

Get Involved!
#############

If you find these ideas interesting, give **Stateful Functions** a try and get involved! Check out the :ref:`Getting Started <getting_started_example>` section for introduction walkthroughs. File an issue if you have an idea how to improve things.