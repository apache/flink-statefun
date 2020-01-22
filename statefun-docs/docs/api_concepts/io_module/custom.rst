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

########################
User Defined I/O Modules
########################

An I/O module provides access to data which is stored in an external system.
If a pre-built I/O module for a particular system does not exist, you can define your own.

Stateful Functions I/O modules are built on top of {flink} connectors, for details of how to build a custom connector see the official {flink} `documentation <https://ci.apache.org/projects/flink/flink-docs-stable>`_.

.. contents:: :local:

A Two Package Approach
======================

Stateful Functions applications are typically modular, containing many modules multiplexed into a single Flink application.
For that reason, I/O modules provide two components, a specification, and an implementation.
That way, multiple modules can depend on the same I/O type while the implementation only needs to be provided once on the classpath.

Specifications
==============

Specifications are the user-facing component of an I/O module and only depend on ``statefun-sdk``.
They include an ingress or egress type and spec.

Ingress and egress types are similar to function types, they provide an namespace and type associated with a class of I/O components.
Specs are what users configure to set properties for a particular instance of an I/O connection.
The only required parameter is the ingress or egress identifier, all other properties will by system specific.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/custom/MyIngressSpec.java
    :language: java
    :lines: 18-

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/custom/MyEgressSpec.java
    :language: java
    :lines: 18-

Implementations
===============

The implementation maps specs to Flink sources and sinks.
They depend on ``statefun-flink-io``, your specifications module, and the underlying Flink connector.

Source and Sink Providers
"""""""""""""""""""""""""

Providers take in the ingress and egress specs and return configured Flink sources and sinks.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/custom/flink/MySourceProvider.java
    :language: java
    :lines: 18-

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/custom/flink/MySinkProvider.java
    :language: java
    :lines: 18-

Flink I/O Module
""""""""""""""""

Flink I/O modules are Stateful Functions' top level entry point for accessing Flink connectors.
They define the relationship between ingress and egress types and source and sink providers.
It also provides runtime configurations through the ``globalConfguration`` which is the union of all configurations in the applications ``flink-conf.yaml`` and any command line arguments passed in the form ``--key value``.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/custom/flink/MyFlinkIoModule.java
    :language: java
    :lines: 18-

I/O modules leverage `Java’s Service Provider Interfaces (SPI) <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_ for discovery.
This means that every JAR should contain a file ``org.apache.flink.statefun.flink.io.spi.FlinkIoModule`` in the ``META_INF/services`` resource directory that lists all available modules that it provides.

.. code-block:: yaml

    org.apache.flink.statefun.docs.impl.io.MyFlinkIoModule