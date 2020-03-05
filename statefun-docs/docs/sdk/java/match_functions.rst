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

###############
Match Functions
###############

Stateful functions provide a powerful abstraction for working with events and state, allowing developers to build components that can react to any kind of message.
Commonly, functions only need to handle a known set of message types, and the ``StatefulMatchFunction`` interface provides an opinionated solution to that problem.

.. contents:: :local:

Common Patterns
===============

Imagine a greeter function that wants to print specialized greeters depending on the type of input.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/match/FnUserGreeter.java
    :language: java
    :lines: 18-

Customers receive one standard message, and employees receive a personalized message depending on whether or not they are managers.
The input is expected to be from a set of known classes.
Certain variants perform some type specific checks and then call the appropriate action.

Simple Match Function
=====================

Stateful match functions are an opinionated variant of stateful functions for precisely this pattern.
Developers outline expected types, optional predicates, and well-typed business logic and let the system dispatch each input to the correct action.
Variants are bound inside a ``configure`` method that is executed once the first time an instance is loaded.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/match/FnMatchGreeter.java
    :language: java
    :lines: 18-

Making Your Function Complete
=============================

Similar to the first example, match functions are partial by default and will throw an ``IllegalStateException`` on any input that does not match any branch.
They can be made complete by providing an ``otherwise`` clause that serves as a catch-all for unmatched input, think of it as a default clause in a Java switch statement.
The ``otherwise`` action takes its message as an untyped ``java.lang.Object``, allowing you to handle any unexpected messages.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/match/FnMatchGreeterWithCatchAll.java
    :language: java
    :lines: 18-
    :emphasize-lines: 15


Action Resolution Order
=======================

Match functions will always match actions from most to least specific using the following resolution rules.

First, find an action that matches the type and predicate. If two predicates will return true for a particular input, the one registered in the binder first wins.
Next, search for an action that matches the type but does not have an associated predicate.
Finally, if a catch-all exists, it will be executed or an ``IllegalStateException`` will be thrown.
