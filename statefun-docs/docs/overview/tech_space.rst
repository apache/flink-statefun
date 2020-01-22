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


.. _tech_space:

##########################
How Does It Compare To...?
##########################

Even though **Stateful Functions** was partially inspired by FaaS offerings, the framework solves a rather different problem with a different approach.

.. contents:: :local:

State-centric
=============

FaaS and serverless application frameworks are **compute-centric**: they elastically scale dedicated resources (processes/containers) for computation. Interacting with state (in a database) and other functions is not their core strength and less well integrated, compared to **Stateful Functions**. A good example of fitting use cases is the classical `Image Resizing with AWS Lambda <https://aws.amazon.com/blogs/compute/resize-images-on-the-fly-with-amazon-s3-aws-lambda-and-amazon-api-gateway>`_.

In comparison, **Stateful Functions** is more **state-centric**: the framework scales state and the interaction between different states and events. Computation exists mainly in the form of the logic that facilitates the interaction of events and states. The sweet spot is event-driven applications that have interacting state machines and need to remember a contextual information, such as the :ref:`Ride Sharing Application example <stateful_functions>`.

These different types of problems need different approaches with different characteristics. **Stateful Functions** were designed to provide a set of properties similar to what characterizes serverless compute, but applied to state-centric problems.

Resource-oblivious Development and Deployment
=============================================

When writing a **Stateful Functions** application, developers do not have to worry about how many parallel instances the application will later run on, or how many resources should be provisioned for the application: this is similar to the FaaS paradigm. Compared to regular Apache Flink applications, there is no need to think in terms of parallelism, slots, or TaskManager resource profiles, to name a few.

Conscious Resource Occupation
=============================

Typical FaaS implementations keep a pool of instances (containers) for a function and scale it up and down according to the event/request rate. In addition, for stateful applications, you need to keep distributed database processes (explicit or abstracted) that scale more slowly.

**Stateful Functions** has a more slow-scaling set of overall resources (Flink TaskManagers), but keeps inside those processes all virtual function instances (with state) of all function types. The resources are shared between the function types depending on the rates of messages sent to them. At any point in time, the vast majority of virtual instances are inactive and not taking any compute/memory resources.

TaskManagers in Stateful Functions can be seen on a similar level as the database processes and the pool of VMs that the FaaS service uses to launch the function processes/containers.

Fast Compute Scaling, Independent of State
==========================================

A stateful FaaS application with a remote database can scale the compute layer (functions) and the state layer (database) independently. **Stateful Functions** implicitly always scales compute and state together. In cases where the compute part does a lot more than only interacting with state (especially CPU intensive operations or blocking interactions with other systems), it makes sense to scale it independently from the state.

Scaling compute and state together makes sense for applications where the compute part is mainly the logic that handles event and state interactions. In those applications, the required compute and state resources are typically strongly correlated, and co-locating the computation and state makes both aspects more efficient, resulting in better scalability.

It is worth noting that it is not a contradiction to combine the two approaches: a “stateful tier” for the parts of the application that deal with state and messaging, and a “stateless tier” for the parts that need to be scaled independently.
