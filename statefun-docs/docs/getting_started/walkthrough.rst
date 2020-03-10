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

.. _walkthrough:

###########
Walkthrough
###########

Like all great introductions in software, this walkthrough will start at the beginning: saying hello.
The application will run a simple function that accepts a request and responds with a greeting.
It will not attempt to cover all the complexities of application development, but instead focus on building a stateful function — which is where you will implement your business logic.

.. contents:: :local:

A Basic Hello
^^^^^^^^^^^^^

Greeting actions are triggered by consuming, routing and passing messages that are defined using ProtoBuf.

.. code-block:: proto

    syntax = "proto3";

    message GreetRequest {
            string who = 1;
    }

    message GreetResponse {
            string who = 1;
            string greeting = 2;
    }


Under the hood, messages are processed using :ref:`stateful functions <java>`, by definition any class that implements the ``StatefulFunction`` interface.

.. code-block:: java

    package org.apache.flink.statefun.examples.greeter;

    import org.apache.flink.statefun.sdk.Context;
    import org.apache.flink.statefun.sdk.StatefulFunction;

    public final class GreetFunction implements StatefulFunction {

            @Override
            public void invoke(Context context, Object input) {
                    GreetRequest greetMessage = (GreetRequest) input;

                    GreetResponse response = GreetResponse.newBuilder()
                            .setWho(greetMessage.getWho())
                            .setGreeting("Hello " + greetMessage.getWho())
                    .build();

                    context.send(GreetingConstants.GREETING_EGRESS_ID, response);
            }
        }

This function takes in a request and sends a response to an external system (or :ref:`egress <egress>`).
While this is nice, it does not show off the real power of stateful functions: handling state.

A Stateful Hello
^^^^^^^^^^^^^^^^

Suppose you want to generate a personalized response for each user depending on how many times they have sent a request.

.. code-block:: java

    private static String greetText(String name, int seen) {
        switch (seen) {
                case 0:
                    return String.format("Hello %s !", name);
            case 1:
                    return String.format("Hello again %s !", name);
            case 2:
                    return String.format("Third times the charm! %s!", name);
            case 3:
                    return String.format("Happy to see you once again %s !", name);
            default:
                    return String.format("Hello at the %d-th time %s", seen + 1, name);
    }

Routing Messages
================

To send a user a personalized greeting, the system needs to keep track of how many times it has seen each user so far.
Speaking in general terms, the simplest solution would be to create one function for every user and independently track the number of times they have been seen. Using most frameworks, this would be prohibitively expensive.
However, stateful functions are virtual and do not consume any CPU or memory when not actively being invoked.
That means your application can create as many functions as necessary — in this case, users — without worrying about resource consumption.

Whenever data is consumed from an external system (or :ref:`ingress <ingress>`), it is routed to a specific function based on a given function type and identifier.
The function type represents the Class of function to be invoked, such as the Greeter function, while the identifier (``GreetRequest#getWho``) scopes the call to a specific virtual instance based on some key.

.. code-block:: java

  package org.apache.flink.statefun.examples.greeter;

  import org.apache.flink.statefun.examples.kafka.generated.GreetRequest;
  import org.apache.flink.statefun.sdk.io.Router;

  final class GreetRouter implements Router<GreetRequest> {

      @Override
      public void route(GreetRequest message, Downstream<GreetRequest> downstream) {
              downstream.forward(GreetingConstants.GREETER_FUNCTION_TYPE, message.getWho(), message);
      }
  }

So, if a message for a user named John comes in, it will be shipped to John’s dedicated Greeter function.
In case there is a following message for a user named Jane, a new instance of the Greeter function will be spawned.

Persistence
===========

:ref:`Persisted value <persisted-value>` is a special data type that enables stateful functions to maintain fault-tolerant state scoped to their identifiers, so that each instance of a function can track state independently.
To “remember” information across multiple greeting messages, you then need to associate a persisted value field (``count``) to the Greet function. For each user, functions can now track how many times they have been seen.

.. code-block:: java

    package org.apache.flink.statefun.examples.greeter;

    import org.apache.flink.statefun.sdk.Context;
    import org.apache.flink.statefun.sdk.StatefulFunction;
    import org.apache.flink.statefun.sdk.annotations.Persisted;
    import org.apache.flink.statefun.sdk.state.PersistedValue;

    public final class GreetFunction implements StatefulFunction {

            @Persisted
            private final PersistedValue<Integer> count = PersistedValue.of("count", Integer.class);

            @Override
            public void invoke(Context context, Object input) {
                GreetRequest greetMessage = (GreetRequest) input;

                GreetResponse response = computePersonalizedGreeting(greetMessage);

                context.send(GreetingConstants.GREETING_EGRESS_ID, response);
            }

            private GreetResponse computePersonalizedGreeting(GreetRequest greetMessage) {
                    final String name = greetMessage.getWho();
                    final int seen = count.getOrDefault(0);
                    count.set(seen + 1);

                    String greeting = greetText(name, seen);

                    return GreetResponse.newBuilder()
                                .setWho(name)
                                .setGreeting(greeting)
                    .build();
            }
    }

Each time a message is processed, the function computes a personalized message for that user.
It reads and updates the number of times that user has been seen and sends a greeting to the egress.

You can check the full code for the application described in this walkthrough `here <{examples}/statefun-greeter-example>`_.
In particular, take a look at the module ``GreetingModule``, which is the main entry point for the full application, to see how everything gets tied together.
You can run this example locally using the provided Docker setup.

.. code-block:: bash

    $ docker-compose build
    $ docker-compose up

Then, send some messages to the topic "names", and observe what comes out of "greetings".

.. code-block:: bash

    $ docker-compose exec kafka-broker kafka-console-producer.sh \
        --broker-list localhost:9092 \
        --topic names

.. code-block:: bash

    $ docker-compose exec kafka-broker kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic greetings

.. image:: ../_static/images/greeter-function.gif
    :align: center

Want To Go Further?
^^^^^^^^^^^^^^^^^^^

This Greeter never forgets a user.
Try and modify the function so that it will reset the ``count`` for any user that spends more than 60 seconds without interacting with the system.

**Hint:** sending messages with a delay is supported, using ``Context#sendAfter``.
How could you use this to implement a periodic check?
