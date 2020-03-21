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
It is defined via an ``IngressIdentifier`` and an ``IngressSpec``.

An ingress identifier, similar to a function type, uniquely identifies an ingress by specifying its input type, a namespace, and a name.

The spec defines the details of how to connect to the external system, which is specific to each individual I/O module. Each identifier-spec pair is bound to the system inside an stateful function module.

.. tabs:: 

    .. group-tab:: Java

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/Identifiers.java
            :language: java
            :lines: 18-

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/ModuleWithIngress.java
            :language: java
            :lines: 18-

    .. group-tab:: Yaml

        .. code-block:: Yaml

           version: "1.0"

           module:
                meta:
                    type: remote
                spec:
                    ingresses:
                    - ingress:
                        meta:
                            id: example/user-ingress      
                            type: # ingress type
                        spec: # ingress specific configurations

Router
""""""

A router is a stateless operator that takes each record from an ingress and routes it to zero or more functions.
Routers are bound to the system via a stateful function module, and unlike other components, an ingress may have any number of routers.

.. tabs::

    .. group-tab:: Java

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/UserRouter.java
            :language: java
            :lines: 18-

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/ingress/ModuleWithRouter.java
            :language: java
            :lines: 18-

    .. group-tab:: Yaml

        When defined in ``yaml``, routers are defined by a list of function types.
        The ``id`` component of the address is pulled from the key associated with each record in its underlying source implementation. 

        .. code-block:: Yaml

            targets:
              - example-namespace/my-function-1
              - example-namespace/my-function-2  
.. _egress:

Egress
^^^^^^

Egress is the opposite of ingress; it is a point that takes messages and writes them to external systems.
Each egress is defined using two components, an ``EgressIdentifier`` and an ``EgressSpec``.

An egress identifier uniquely identifies an egress based on a namespace, name, and producing type.
An egress spec defines the details of how to connect to the external system, the details are specific to each individual I/O module.
Each identifier-spec pair are bound to the system inside a stateful function module.

.. tabs::

    .. group-tab:: Java

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/egress/Identifiers.java
            :language: java
            :lines: 18-

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/egress/ModuleWithEgress.java
            :language: java
            :lines: 18-

    .. group-tab:: Yaml

        .. code-block:: Yaml 
    
            version: "1.0"

            module:
                meta:
                    type: remote
                spec:
                    egresses:
                    - egress:
                        meta:
                            id: example/user-egress
                            type: # egress type
                        spec: # egress specific configurations

Stateful functions may then message an egress the same way they message another function, passing the egress identifier as function type.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/egress/FnOutputting.java
    :language: java
    :lines: 18-
