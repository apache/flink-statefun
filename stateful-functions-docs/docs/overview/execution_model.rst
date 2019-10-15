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

.. _execution_model:

###########################
Distributed Execution Model
###########################

.. contents:: :local:

The Apache Flink Dataflow
^^^^^^^^^^^^^^^^^^^^^^^^^

All ``Functions``, ``Ingresses``, ``Egresses`` and ``Routers`` get assembled into a single Flink application, backed by a single dataflow. This is a different approach from regular Apache Flink applications, where each transformation function is an individual operator in the dataflow. In those applications, each operator has dedicated resources in the TaskManagers (i.e. slots, threads, memory) and can only send events to operators downstream. Assembling all functions into a single, fixed-structure Flink job ensures that these do not require pre-reserved resources and allows **Stateful Functions** to realize arbitrary function-to-function messaging. The fixed-structure graph represents the "state and messaging fabric", rather than capturing the topology with which different function types interact.

.. image:: ../_static/images/flink_dataflow_graph.png
   :width: 50%
   :align: center

The Apache Flink dataflow executes the **Stateful Functions** application as follows:

* Ingresses and Routers run in `Flink Source Functions <https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.html>`_, Egresses in `Flink Sink Functions <https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/java/org/apache/flink/streaming/api/functions/sink/SinkFunction.html>`_.

* All functions are executed by the ``FunctionGroupOperator``. The logical ID of each function acts as the key by which the operator is parallelized. Functions and their state are hence sharded by their logical ID.

* All messages from Routers to functions and from function to function are sent through ``keyBy()`` streams that route the message according to the key (i.e. the logical ID).

* Arbitrary function-to-function messaging is implemented by a feedback loop:

  * Functions send messages downstream, routed by key to the ``Feedback Operator``.
  * The Feedback Operator is co-located shard-per-shard with the Function Dispatcher and places the messages into an in-memory feedback channel.
  * The Function Dispatcher pulls messages out of the feedback channel and dispatches them to the functions.
  * When a function emits a message to an Egress, it “side outputs” that message to the respective sink function.

Logical Function Instances
^^^^^^^^^^^^^^^^^^^^^^^^^^
All functions types and instances run within the ``FunctionGroupOperator``. Each parallel operator maintains a single instance for each function type and the state for all functions is stored in Flink’s state backend — typically backed by RocksDB or some other map implementation. When receiving a message for a function, the State Dispatcher looks up the function instance from the function type, and the state from the function type and logical ID. The operator then maps the state into the function instance and invokes the function with the message. Any updates that the function performs on the state are mapped back into the Flink state backend.

.. image:: ../_static/images/flink_function_multiplexing.png
   :width: 75%
   :align: center

Parallel Execution & Fault Tolerance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Apache Flink executes the dataflow in parallel by distributing the operator instances across different parallel processes (i.e. TaskManagers) and streaming events between them. State storage and failure recovery are backed by Flink's state and fault tolerance mechanisms. However, Flink does not support fault tolerant loops out-of-the-box — the **Stateful Functions** implementation extends Flink's native snapshot-based fault tolerance mechanism to support cyclic data flow graphs, following an approach similar to the one outlined in `this paper <https://pdfs.semanticscholar.org/6fa0/917417d3c213b0e130ae01b7b440b1868dde.pdf>`_.
