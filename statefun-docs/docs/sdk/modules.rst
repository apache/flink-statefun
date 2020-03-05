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

.. _modules:

#######
Modules
#######

Stateful Function applications are composed of one or more ``Modules``.
A module is a bundle of functions that are loaded by the runtime and available to be messaged.
Functions from all loaded modules are multiplexed and free to message each other arbitrarily.

Stateful Functions supports two types of modules: Embedded and Remote.

.. contents:: :local:

.. _embedded_module:

Embedded Module
===============

Embedded modules are co-located with, and embedded within, the {flink} runtime.

This module type only supports JVM based languages and are defined by implementing the ``StatefulFunctionModule`` interface.
Embedded modules offer a single configuration method where stateful functions are bound to the system based on their :ref:`function type <address>`.
Runtime configurations are available through the ``globalConfiguration``, which is the union of all configurations in the applications ``flink-conf.yaml`` under the prefix ``statefun.module.global-config`` and any command line arguments passed in the form ``--key value``.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/BasicFunctionModule.java
    :language: java
    :lines: 18-

Embedded modules leverage `Java’s Service Provider Interfaces (SPI) <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_ for discovery.
This means that every JAR should contain a file ``org.apache.flink.statefun.sdk.spi.StatefulFunctionModule`` in the ``META_INF/services`` resource directory that lists all available modules that it provides.

.. code-block:: yaml

    org.apache.flink.statefun.docs.BasicFunctionModule

.. _remote_module:

Remote Module
=============

Remote modules are run as external processes from the {flink} runtime; in the same container, as a sidecar, or other external location.

This module type can support any number of language SDK's.
Remote modules are registered with the system via ``YAML`` configuration files.

.. literalinclude:: ../../src/main/resources/module.yaml
    :language: yaml
    :lines: 16-

Remote module definitions eagerly register each function type, the endpoint under which the function is available, along with all states for a particular function.
