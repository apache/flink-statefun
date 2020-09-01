---
title: SDK 
nav-id: sdk
nav-pos: 3
nav-title: 'SDK'
nav-parent_id: root
nav-show_overview: true 
permalink: /sdk/index.html
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

Stateful Functions applications are composed of one or more modules.
A module is a bundle of functions loaded by the runtime and made available to be messaged.
Functions from all loaded modules are multiplexed and free to message each other arbitrarily.

Stateful Functions supports two types of modules: Remote and Embedded.

* This will be replaced by the TOC
{:toc}

## Remote Module

Remote modules run as external processes from the Apache Flink® runtime; in the same container, as a sidecar, using a serverless platform or other external location.
This module type can support any number of language SDKs.
Remote modules are registered with the system via ``YAML`` configuration files.

### Specification

A remote module configuration consists of a ``meta`` section and a ``spec`` section.
The ``meta`` contains auxiliary information about the module; 
while ``spec`` describes the functions contained within the module and defines their persisted values.

### Defining Functions

``module.spec.functions`` declares a list of ``function`` objects that are implemented by the remote module.
A ``function`` is described via a number of properties:

* ``function.meta.kind``
    * The protocol used to communicate with the remote function.
    * Supported values - ``http``
* ``function.meta.type``
    * The function type, defined as ``<namespace>/<name>``.
* ``function.spec.endpoint``
    * The endpoint at which the function is reachable.
    * Supported schemes: ``http``, ``https``.
    * Transport via UNIX domain sockets is supported by using the schemes ``http+unix`` or ``https+unix``.
    * When using UNIX domain sockets, the endpoint format is: ``http+unix://<socket-file-path>/<serve-url-path>``. For example, ``http+unix:///uds.sock/path/of/url``.
* ``function.spec.states``
    * A list of the persisted values declared within the remote function.
    * Each entry consists of a `name` property and an optional `expireAfter` property.
    * Default for `expireAfter` - 0, meaning that state expiration is disabled.
* ``function.spec.maxNumBatchRequests``
    * The maximum number of records that can be processed by a function for a particular ``address`` before invoking backpressure on the system.
    * Default: 1000
* ``function.spec.timeout``
    * The maximum amount of time for the runtime to wait for the remote function to return before failing.
      This spans the complete call, including connecting to the function endpoint, writing the request, function processing, and reading the response.
    * Default: 1 min
* ``function.spec.connectTimeout``
    * The maximum amount of time for the runtime to wait for connecting to the remote function endpoint.
    * Default: 10 sec
* ``function.spec.readTimeout``
    * The maximum amount of time for the runtime to wait for individual read IO operations, such as reading the invocation response.
    * Default: 10 sec
* ``function.spec.writeTimeout``
    * The maximum amount of time for the runtime to wait for individual write IO operations, such as writing the invocation request.
    * Default: 10 sec

### Full Example

{% highlight yaml %}
version: "2.0"

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
            - name: seen_count
              expireAfter: 5min
          maxNumBatchRequests: 500
          timeout: 2min
{% endhighlight %}

## Embedded Module

Embedded modules are co-located with, and embedded within, the Apache Flink® runtime.

This module type only supports JVM-based languages and is defined by implementing the ``StatefulFunctionModule`` interface.
Embedded modules offer a single configuration method where stateful functions bind to the system based on their
[function type]({{ site.baseurl }}/concepts/logical.html#function-address).
Runtime configurations are available through the ``globalConfiguration``, which is the union of all configurations
in the applications ``flink-conf.yaml`` under the prefix ``statefun.module.global-config``, and any command line
arguments passed in the form ``--key value``.

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

