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

.. _python_walkthrough:

##################
Python Walkthrough
##################

Many buisnesses need to track the ongoing actions of their customers.
The walkthrough will implement a simple function that accepts login events and responds with the number of times a user has logged in over time.
It will not attempt to cover all the complexities of application development, but instead focus on building a stateful function — which is where you will implement your business logic.

.. contents:: :local:

Basic Event Processing
======================

Login actions are triggered by consuming, routing and passing messages that are defined using ProtoBuf.

.. code-block:: proto

    syntax = "proto3";

    message LoginEvent {
        string user_name = 1;
    }

    message SeenCount {
        int64 seen = 1;
    }

Under the hood, messages are processed using :ref:`stateful functions <python>`, by definition any function that it bound to the ``StatefulFunction`` API.

.. code-block:: java

    from messages_pb2 import LoginEvent
    from messages_pb2 import SeenCount

    from statefun import StatefulFunctions

    from statefun import RequestReplyHandler
    from statefun import kafka_egress_builder

    functions = StatefulFunctions()

    @functions.bind("k8s-demo/greeter")
    def greet(context, message: LoginEvent):
        state = SeenCount()
        state.seen = 1

        egress_message = kafka_egress_builder(topic="seen", key=message.user_name, value=state)
        context.pack_and_send_egress("k8s-demo/greets-egress", egress_message)


This function takes in a request and sends a response to an external system (or :ref:`egress <egress>`).
While this is nice, it does not show off the real power of stateful functions: handling state.

Routing Messages
================

Suppose you want to generate a personalized response for each user depending on how many times they have sent a request.
The system needs to keep track of how many times it has seen each user so far.
Speaking in general terms, the simplest solution would be to create one function for every user and independently track the number of times they have been seen. 
Using most frameworks, this would be prohibitively expensive.
However, stateful functions are virtual and do not consume any CPU or memory when not actively being invoked.
That means your application can create as many functions as necessary — in this case, users — without worrying about resource consumption.

Whenever data is consumed from an external system (or :ref:`ingress <ingress>`), it is routed to a specific function based on a given function type and identifier.
The function type represents the Class of function to be invoked, such as the ``greeter`` function, while the identifier (``LoginEvent#user_name``) scopes the call to a specific virtual instance based on some key.

When defined in ``Yaml``, routers are configured as a list of target function types.
The ``id`` is based on the key pulled from the underlying source, such as a kafka partition key. 

.. code-block:: yaml

  targets:
    - k8s-demo/greeter

So, if a message for a user named John comes in, it will be shipped to John’s dedicated ``greeter`` function.
In case there is a following message for a user named Jane, a new instance of the ``greeter`` function will be spawned.

Persistence
===========

:ref:`Persisted values <python_persistence>` is a feature that enables stateful functions to maintain fault-tolerant state scoped to their identifiers, so that each instance of a function can track state independently.
To “remember” information across multiple login messages, you then need to associate a persisted value (``seen_count``) to the function.
For each user, functions can now track how many times they have logged in.

.. code-block:: python

    from messages_pb2 import LoginEvent
    from messages_pb2 import SeenCount

    from statefun import StatefulFunctions

    from statefun import RequestReplyHandler
    from statefun import kafka_egress_builder

    functions = StatefulFunctions()

    @functions.bind("k8s-demo/greeter")
    def greet(context, message: LoginEvent):
        state = context.state('seen_count').unpack(SeenCount)
        if not state:
            state = SeenCount()
            state.seen = 1
        else:
            state.seen += 1
        context.state('seen_count').pack(state)

        egress_message = kafka_egress_builder(topic="seen", key=message.user_name, value=state)
        context.pack_and_send_egress("k8s-demo/greets-egress", egress_message)


Each time a message is processed, the function updates and returns the number of times they have previously logged in.

You can check the full code for the application described in this walkthrough `here <{examples}/statefun-python-k8s>`_.
In particular, take a look at the files ``main.py`` and ``module.yaml`` which contain the buisness logic and configurations for the full application respectively, to see how everything gets tied together.
You can run this example on a Kubernetes cluster using the provided Helm charts. 

Want To Go Further?
===================

This Greeter never forgets a user.
Try and modify the function so that it will reset the ``seen_count`` for any user that spends more than 60 seconds without interacting with the system.

**Hint:** sending messages with a delay is supported, using ``context.send_after``.
How could you use this to implement a periodic check?
