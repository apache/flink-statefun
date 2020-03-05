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

###########
Persistence
###########

Stateful Functions treats state as a first class citizen and so all stateful functions can easily define state that is automatically made fault tolerant by the runtime.

.. contents:: :local:

.. _persisted-value:

Defining a Persistent Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All stateful functions may contain state by merely defining one or more ``PersistedValue`` fields.
A ``PersistedValue`` is defined by its name and the class of the type that it stores. 
The data is always scoped to a specific function type and identifier.
Below is a stateful function that greets users based on the number of times they have been seen.

.. warning::

    All ``PersistedValue`` fields must be marked with an ``@Persisted`` annotation or they will not be made fault tolerant by the runtime.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/FnUserGreeter.java
    :language: java
    :lines: 18-

Persisted value comes with the right primitive methods to build powerful stateful applications.
Calling ``PersistedValue#get`` will return the current value of an object stored in state, or ``null`` if nothing is set.
Conversely, ``PersistedValue#set`` will update the value in state and ``PersistedValue#clear`` will delete the value from state.


Supported Types
^^^^^^^^^^^^^^^

Stateful Functions applications are typically designed to run indefinitely or for long periods of time.
As with all long-running services, the applications need to be updated to adapt to changing requirements.
This goes the same for data schemas that the applications work against; they evolve along with the application.
That is why the system limits the types that can be stored inside a ``PersistedValue`` to all Java primitives and complex types that support well defined schema migration semantics.

.. note::

    Schema evolution is supported naturally with protobuf and json, and the project is working on connecting it to Flink’s schema evolution capabilities.

POJO types
""""""""""

Stateful Functions recognizes data types as a POJO type if the following conditions are fulfilled:

* The class is public and standalone (no non-static inner class)
* The class has a public no-argument constructor
* All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final) or have a public getter- and a setter- method that follows the Java beans naming conventions for getters and setters.

Apache Avro
"""""""""""

Stateful Functions can store any Apache Avro class and fully supports evolving schema of Avro type state, as long as the schema change is considered compatible by `Avro’s rules for schema resolution <https://avro.apache.org/docs/current/spec.html#Schema+Resolution>`_.

Protocol Buffers
""""""""""""""""

Stateful Functions can store any `Protocol Buffer <https://developers.google.com/protocol-buffers/>`_ class and fully supports schema evolution as long as the schema change is considered compatible by ProtoBuff's rules for schema evolution.

Json
""""

Stateful Functions can store any object that serializes as JSON.
