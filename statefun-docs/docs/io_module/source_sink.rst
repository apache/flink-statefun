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

################
Flink Connectors
################

The source-sink I/O module allows you to plug in existing, or custom, Flink connectors that are not already integrated into a dedicated I/O module.
For details details of how to build a custom connector see the official {flink} `documentation <https://ci.apache.org/projects/flink/flink-docs-stable>`_.

Dependency
==========

To use the Source/Sink I/O Module, please include the following dependency in your pom.

.. code-block:: xml

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>statefun-flink-io</artifactId>
        <version>{version}</version>
        <scope>provided</scope>
    </dependency>

Source Spec
===========

A source function spec creates an ingress from a Flink source function.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/flink/ModuleWithSourceSpec.java
    :language: java
    :lines: 18-

Sink Spec
=========

A sink function spec creates an egress from a Flink sink function.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/flink/ModuleWithSinkSpec.java
    :language: java
    :lines: 18-
