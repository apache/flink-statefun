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

##########
I/O Module
##########

.. toctree::
  :hidden:

  kafka
  source_sink
  custom

Stateful Functions' I/O modules allow functions to receive and send messages to external systems.
Based on the concept of Ingress (input) and Egress (output) points, and built on top of the {flink} connector ecosystem, I/O modules enable functions to interact with the outside world through the style of message passing.

.. contents:: :local:

.. _ingress:

Ingress
^^^^^^^^

An Ingress is an input point where data is consumed from an external system and forwarded to zero or more functions.
An ``IngressIdentifier`` and an ``IngressSpec`` define it.

An ingress identifier, similar to a function type, uniquely identifies an ingress by specifying its input type, a namespace, and a name.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/Identifiers.java
    :language: java
    :lines: 18-

The spec defines the details of how to connect to the external system, which is specific to each individual I/O module.
Each identifier-spec pair is bound to the system inside an stateful function module.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/ModuleWithIngress.java
    :language: java
    :lines: 18-

Router
""""""

A router is a stateless operator that takes each record from an ingress and routes it to zero or more functions.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/UserRouter.java
    :language: java
    :lines: 18-

Routers are bound to the system via a stateful function module.
Unlike other components, an ingress may have any number of routers.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/ModuleWithRouter.java
    :language: java
    :lines: 18-

.. _egress:

Egress
^^^^^^

Egress is the opposite of ingress; it is a point that takes messages and writes them to external systems.
Each egress is defined using two components, an ``EgressIdentifier`` and an ``EgressSpec``.

An egress identifier uniquely identifies an egress based on a namespace, name, and producing type.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/egress/Identifiers.java
    :language: java
    :lines: 18-

An egress spec defines the details of how to connect to the external system, the details are specific to each individual I/O module.
Each identifier-spec pair are bound to the system inside a stateful function module.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/egress/ModuleWithEgress.java
    :language: java
    :lines: 18-

Stateful functions may then message an egress the same way they message another function, passing the egress identifier as function type.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/egress/FnOutputting.java
    :language: java
    :lines: 18-

I/O modules leverage `Java’s Service Provider Interfaces (SPI) <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_ for discovery.
This means that every JAR should contain a file ``org.apache.flink.statefun.sdk.spi.StatefulFunctionModule`` in the ``META_INF/services`` resource directory that lists all available modules that it provides.

.. code-block:: yaml

    BasicFunctionModule
