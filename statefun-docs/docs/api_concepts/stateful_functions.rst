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

.. _statefun:

##################
Stateful Functions
##################

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.

.. contents:: :local:

Defining A Stateful Function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A stateful function is any class that implements the ``StatefulFunction`` interface. The following is an example of a simple hello world function.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/FnHelloWorld.java
    :language: java
    :lines: 18-

Functions process each incoming message through their ``invoke`` method.
Input's are untyped and passed through the system as a ``java.lang.Object`` so one function can potentially process multiple types of messages.

The ``Context`` provides metadata about the current message and function, and is how you can call other functions or external systems.
Functions are invoked based on a function type and unique identifier.

Function Type's and Identifiers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In a local environment, the address of an object is the same as a reference to it.
But in a distributed system, objects may be spread across multiple machines and may or may not be active at any given moment.

In Stateful Functions, function types and identifiers are used to reference specific stateful functions in the system.
A function type is similar to a class in an object-oriented language; it declares what sort of function the address references.
The id is a primary key and scopes the function call to a specific instance of the function type.

Suppose a Stateful Functions application was tracking metadata about each user account on a website.
The system would contain a user stateful function that accepts and responds to inputs about users and tracks relevant information.
Stateful Functions will create one virtual instance of this stateful function for every user.
Other functions can call the function for any particular user by the user function type and using the current user id as the instance identifier.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/FnUser.java
    :language: java
    :emphasize-lines: 10
    :lines: 18-

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/FnCaller.java
    :language: java
    :emphasize-lines: 14
    :lines: 18-

Virtual Functions
^^^^^^^^^^^^^^^^^

Functions are virtual, which means the system can support an infinite number of active functions while only requiring a static number of physical objects on the JVM heap.
Any function can call any other without ever triggering an allocation.
The system will make it appear as if functions are always available in-memory.

Stateful Functions applications deploy on Apache Flink's horizontally parallel runtime.
If the user function, seen above, is run on a Flink cluster with a parallelism of 10, then only ten objects will ever be allocated.
Even if the application creates a billion user functions for a billion different users, memory usage will be stable.
Those billion virtual functions will be evenly partitioned and run by the ten underlying objects.
New object creation only occurs the first time a function of that type, regardless of id, is needed.

Sending Delayed Messages
^^^^^^^^^^^^^^^^^^^^^^^^

Functions are able to send messages on a delay so that they will arrive after some duration.
Functions may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking so functions will continue to process records between the time a delayed message is sent and received.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/delay/FnDelayedMessage.java
    :language: java
    :lines: 18-

Completing Async Requests
^^^^^^^^^^^^^^^^^^^^^^^^^

When interacting with external systems, such as a database or API, one needs to take care that communication delay with the external system does not dominate the application’s total work.
Stateful Functions allows registering a java ``CompletableFuture`` that will resolve to a value at some point in the future.
Future's are registered along with a metadata object that provides additional context about the caller.

When the future completes, either successfully or exceptionally, the caller function type and id will be invoked with a ``AsyncOperationResult``.
An asynchronous result can complete in one of three states:

Success
=======

The asynchronous operation has succeeded, and the produced result can be obtained via ``AsyncOperationResult#value``.

Failure
=======

The asynchronous operation has failed, and the cause can be obtained via ``AsyncOperationResult#throwable``.

Unknown
=======

The stateful function was restarted, possibly on a different machine, before the ``CompletableFuture`` was completed, therefore it is unknown what is the status of the asynchronous operation.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/async/EnrichmentFunction.java
    :language: java
    :lines: 18-

Function Providers and Dependency Injection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Stateful functions are created across a distributed cluster of nodes.
``StatefulFunctionProvider`` is a factory class for creating a new instance of a stateful function the first time it is activated.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/CustomProvider.java
    :language: java
    :lines: 18-

Providers are called once per type on each parallel worker, not for each id.
If a stateful function requires custom configurations, they can be defined inside a provider and passed to the functions' constructor.
This is also where shared physical resources, such as a database connection, can be created that are used by any number of virtual functions.
Now, tests can quickly provide mock, or test dependencies, without the need for complex dependency injection frameworks.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/FunctionTest.java
    :language: java
    :lines: 18-

.. _module:

Stateful Function Modules
^^^^^^^^^^^^^^^^^^^^^^^^^

Modules define a Stateful Functions application's top-level entry point and are where everything gets tied together.
They offer a single configuration method where stateful functions are bound to the system.
It also provides runtime configurations through the ``globalConfguration`` which is the union of all configurations in the applications ``flink-conf.yaml`` under the prefix ``statefun.module.global-config`` and any command line arguments passed in the form ``--key value``.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/BasicFunctionModule.java
    :language: java
    :lines: 18-

Modules leverage `Java’s Service Provider Interfaces (SPI) <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_ for discovery.
This means that every JAR should contain a file ``org.apache.flink.statefun.sdk.spi.StatefulFunctionModule`` in the ``META_INF/services`` resource directory that lists all available modules that it provides.

.. code-block:: yaml

    BasicFunctionModule
