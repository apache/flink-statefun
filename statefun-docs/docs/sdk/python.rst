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

.. _python:

######
Python
######

Stateful functions are the building blocks of applications; they are atomic units of isolation, distribution, and persistence.
As objects, they encapsulate the state of a single entity (e.g., a specific user, device, or session) and encode its behavior.
Stateful functions can interact with each other, and external systems, through message passing.
The Python SDK is supported as a :ref:`remote_module`.

To get started, add the Python SDK as a dependency to your application.

.. code-block:: bash

    apache-flink-statefun=={version}

.. contents:: :local:

Defining A Stateful Function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A stateful function is any function that that takes two parameters, a ``context`` and ``message``.
The function is bound to the runtime through the stateful functions decorator.
The following is an example of a simple hello world function.

.. literalinclude:: ../../src/main/python/HelloWorld.py
    :language: python
    :lines: 19-

This code declares a function with in the namespace ``flink`` and of type ``hello`` and binds it to the ``hello_function`` Python instance.

Messages's are untyped and passed through the system as ``google.protobuf.Any`` so one function can potentially process multiple types of messages.

The ``context`` provides metadata about the current message and function, and is how you can call other functions or external systems.
A full reference of all methods supported by the context object are listed at the :ref:`bottom of this page <context_reference>`.

Type Hints
==========

If the function has a static set of known supported types, they may be specified as `type hints <https://docs.python.org/3/library/typing.html>`_.
This includes `union types <https://docs.python.org/3/library/typing.html#typing.Union>`_ for functions that support multiple input message types.


.. literalinclude:: ../../src/main/python/TypeHint.py
    :language: python
    :lines: 19-

Function Types and Messaging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The decorator ``bind`` registers each function with the runtime under a function type.
The function type must take the form ``<namespace>/<name>``.
Function types can then be referenced from other functions to create an address and message a particular instance.

.. literalinclude:: ../../src/main/python/Caller.py
    :language: python
    :lines: 19-

Sending Delayed Messages
^^^^^^^^^^^^^^^^^^^^^^^^

Functions are able to send messages on a delay so that they will arrive after some duration.
Functions may even send themselves delayed messages that can serve as a callback.
The delayed message is non-blocking so functions will continue to process records between the time a delayed message is sent and received.
The delay is specified via a `Python timedelta <https://docs.python.org/3/library/datetime.html#datetime.timedelta>`_.

.. literalinclude:: ../../src/main/python/Delayed.py
    :language: python
    :lines: 19-

Persistence
^^^^^^^^^^^
Stateful Functions treats state as a first class citizen and so all stateful functions can easily define state that is automatically made fault tolerant by the runtime.
All stateful functions may contain state by merely storing values within the ``context`` object.
The data is always scoped to a specific function type and identifier.
State values could be absent, ``None``, or a ``google.protobuf.Any``.

.. warning::

    :ref:`remote_module`'s require that all state values are eagerly registered at ``module.yaml``.

Below is a stateful function that greets users based on the number of times they have been seen.

.. literalinclude:: ../../src/main/python/Counter.py
    :language: python
    :lines: 19-

Additionally, persisted values may be cleared by deleting its value.

.. code-block:: python

    del context["count"]


Exposing Functions
^^^^^^^^^^^^^^^^^^

The Python SDK ships with a ``RequestReplyHandler`` that automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The ``RequestReplyHandler`` may be exposed using any HTTP framework.

.. code-block:: python

    from statefun import RequestReplyHandler

    handler RequestReplyHandler(functions)

Serving Functions With Flask
============================

One popular Python web framework is `Flask <https://palletsprojects.com/p/flask/>`_.
It can be used to quickly and easily expose a ``RequestResponseHandler``.

.. code-block:: python

    @app.route('/statefun', methods=['POST'])
    def handle():
        response_data = handler(request.data)
        response = make_response(response_data)
        response.headers.set('Content-Type', 'application/octet-stream')
        return response


    if __name__ == "__main__":
        app.run()

.. _context_reference:

Context Reference
^^^^^^^^^^^^^^^^^

The ``context`` object passed to each function has the following attributes / methods.

* send(self, typename: str, id: str, message: Any)
    * Send a message to any function with the function type of the the form ``<namesapce>/<type>`` and message of type ``google.protobuf.Any``
* pack_and_send(self, typename: str, id: str, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* reply(self, message: Any)
    * Sends a message to the invoking function
* pack_and_reply(self, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* send_after(self, delay: timedelta, typename: str, id: str, message: Any)
    * Sends a message after a delay
* pack_and_send_after(self, delay: timedelta, typename: str, id: str, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* send_egress(self, typename, message: Any)
    * Emits a message to an egress with a typename of the form ``<namespace>/<name>``
* pack_and_send_egress(self, typename, message)
    * The same as above, but it will pack the protobuf message in an ``Any``
* __getitem__(self, name)
    * Retrieves the state registered under the name as an ``Any`` or ``None`` if no value is set
* __delitem__(self, name)
    * Deletes the state registered under the name
* __setitem__(self, name, value: Any)
    * Stores the value under the given name in state.
