---
title: Modules 
nav-id: modules
nav-pos: 3
nav-title: Modules
nav-parent_id: sdk
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
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
-->

Stateful Function applications are composed of one or more ``Modules``.
A module is a bundle of functions that are loaded by the runtime and available to be messaged.
Functions from all loaded modules are multiplexed and free to message each other arbitrarily.

Stateful Functions supports two types of modules: Embedded and Remote.

* This will be replaced by the TOC
{:toc}

## Embedded Module

Embedded modules are co-located with, and embedded within, the {flink} runtime.

This module type only supports JVM based languages and are defined by implementing the ``StatefulFunctionModule`` interface.
Embedded modules offer a single configuration method where stateful functions are bound to the system based on their [function type]({{ site.baseurl }}/concepts/logical.html#function-address).
Runtime configurations are available through the ``globalConfiguration``, which is the union of all configurations in the applications ``flink-conf.yaml`` under the prefix ``statefun.module.global-config`` and any command line arguments passed in the form ``--key value``.

{% highlight java %}
package org.apache.flink.statefun.docs;

import java.util.Map;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class BasicFunctionModule implements StatefulFunctionModule {

	public void configure(Map<String, String> globalConfiguration, Binder binder) {

		// Declare the user function and bind it to its type
		binder.bindFunctionProvider(FnWithDependency.TYPE, new CustomProvider());

		// Stateful functions that do not require any configuration
		// can declare their provider using java 8 lambda syntax
		binder.bindFunctionProvider(Identifiers.HELLO_TYPE, unused -> new FnHelloWorld());
	}
}
{% endhighlight %}

Embedded modules leverage [Java’s Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) for discovery.
This means that every JAR should contain a file ``org.apache.flink.statefun.sdk.spi.StatefulFunctionModule`` in the ``META_INF/services`` resource directory that lists all available modules that it provides.

{% highlight none %}
org.apache.flink.statefun.docs.BasicFunctionModule
{% endhighlight %}

## Remote Module

Remote modules are run as external processes from the {flink} runtime; in the same container, as a sidecar, or other external location.

This module type can support any number of language SDK's.
Remote modules are registered with the system via ``YAML`` configuration files.

### Specification

A remote module configuration consists of a ``meta`` section and a ``spec`` section.
``meta`` contains auxillary information about the module.
The ``spec`` describes the functions contained within the module and defines their persisted values.

### Defining Functions

``module.spec.functions`` declares a list of ``function`` objects that are implemented by the remote module.
A ``function`` is described via a number of properties.

* ``function.meta.kind``
    * The protocol used to communicate with the remote function.
    * Supported Values - ``http``
* ``function.meta.type``
    * The function type, defined as ``<namespace>/<name>``.
* ``function.spec.endpoint``
    * The endpoint at which the function is reachable.
* ``function.spec.states``
    * A list of the names of the persisted values decalred within the remote function.
* ``function.spec.maxNumBatchRequests``
    * The maximum number of records that can be processed by a function for a particular ``address`` before invoking backpressure on the system.
    * Default - 1000
* ``function.spec.timeout``
    * The maximum amount of time for the runtime to wait for the remote function to return before failing.
    * Default - 1 min

### Full Example

{% highlight yaml %}
version: "1.0"

module:
  meta:
    type: remote
  spec:
    functions:
      - function:
        meta:
          kind: http
          type: example/greeter
        spec:
          endpoint: http://<host-name>/statefun
          states:
            - seen_count
          maxNumBatchRequests: 500
          timeout: 2min
{% endhighlight %}
