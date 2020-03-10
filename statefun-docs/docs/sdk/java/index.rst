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

.. _java:

####
Java 
####

.. toctree::
  :hidden:

  match_functions

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.
The Java SDK is supported as an :ref:`embedded_module`.

To get started, add the Java SDK as a dependency to your application.

.. code-block:: xml

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>statefun-sdk</artifactId>
        <version>{version}</version>
        <scope>provided</scope>
    </dependency>

.. contents:: :local:

Defining A Stateful Function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A stateful function is any class that implements the ``StatefulFunction`` interface.
The following is an example of a simple hello world function.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/FnHelloWorld.java
    :language: java
    :lines: 18-

Functions process each incoming message through their ``invoke`` method.
Input's are untyped and passed through the system as a ``java.lang.Object`` so one function can potentially process multiple types of messages.

The ``Context`` provides metadata about the current message and function, and is how you can call other functions or external systems.
Functions are invoked based on a function type and unique identifier.

Function Types and Messaging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In Java, function types are defined as a _stringly_ typed reference containing a namespace and name.
The type is bound to the implementing class in the :ref:`module <embedded_module>` definition.
Below is an example function type for the hello world function.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/Identifiers.java
    :language: java
    :lines: 18-

This type can then be referenced from other functions to create an address and message a particular instance.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/FnCaller.java
    :language: java
    :emphasize-lines: 12
    :lines: 18-

Sending Delayed Messages
^^^^^^^^^^^^^^^^^^^^^^^^

Functions are able to send messages on a delay so that they will arrive after some duration.
Functions may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking so functions will continue to process records between the time a delayed message is sent and received.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/delay/FnDelayedMessage.java
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

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/async/EnrichmentFunction.java
    :language: java
    :lines: 18-

.. _persisted-value:

Persistence
^^^^^^^^^^^
Stateful Functions treats state as a first class citizen and so all stateful functions can easily define state that is automatically made fault tolerant by the runtime.
All stateful functions may contain state by merely defining one or more persisted fields.

The simplest way to get started is with a ``PersistedValue``, which is defined by its name and the class of the type that it stores.
The data is always scoped to a specific function type and identifier.
Below is a stateful function that greets users based on the number of times they have been seen.

.. warning::

    All ``PersistedValue``, ``PersistedTable``, and ``PersistedAppendingBuffer`` fields must be marked with an ``@Persisted`` annotation or they will not be made fault tolerant by the runtime.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/FnUserGreeter.java
    :language: java
    :lines: 18-

Persisted value comes with the right primitive methods to build powerful stateful applications.
Calling ``PersistedValue#get`` will return the current value of an object stored in state, or ``null`` if nothing is set.
Conversely, ``PersistedValue#set`` will update the value in state and ``PersistedValue#clear`` will delete the value from state.

Collection Types
================

Along with ``PersistedValue``, the Java SDK supports two persisted collection types.
``PersistedTable`` is a collection of keys and values, and ``PersistedAppendingBuffer`` is an append-only buffer.

These types are functionally equivalent to ``PersistedValue<Map>`` and ``PersistedValue<Collection>`` respectively but may provide better performance in some situations.

.. code-block:: java

    @Persisted
    PersistedTable<String, Integer> table =
        PersistedTable.of("my-table", String.class, Integer.class);

    @Persisted
    PersistedAppendingBuffer<Integer> buffer =
        PersistedAppendingBuffer.of("my-buffer", Integer.class);


Function Providers and Dependency Injection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Stateful functions are created across a distributed cluster of nodes.
``StatefulFunctionProvider`` is a factory class for creating a new instance of a stateful function the first time it is activated.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/CustomProvider.java
    :language: java
    :lines: 18-

Providers are called once per type on each parallel worker, not for each id.
If a stateful function requires custom configurations, they can be defined inside a provider and passed to the functions' constructor.
This is also where shared physical resources, such as a database connection, can be created that are used by any number of virtual functions.
Now, tests can quickly provide mock, or test dependencies, without the need for complex dependency injection frameworks.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/FunctionTest.java
    :language: java
    :lines: 18-
