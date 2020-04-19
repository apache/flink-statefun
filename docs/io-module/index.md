---
title: I/O Module 
nav-id: io-module
nav-pos: 4
nav-title: 'I/O Module'
nav-parent_id: root
nav-show_overview: true 
permalink: /io-module/index.html
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

Stateful Functions' I/O modules allow functions to receive and send messages to external systems.
Based on the concept of Ingress (input) and Egress (output) points, and built on top of the Apache Flink® connector ecosystem, I/O modules enable functions to interact with the outside world through the style of message passing.

* This will be replaced by the TOC
{:toc}

## Ingress

An Ingress is an input point where data is consumed from an external system and forwarded to zero or more functions.
It is defined via an ``IngressIdentifier`` and an ``IngressSpec``.

An ingress identifier, similar to a function type, uniquely identifies an ingress by specifying its input type, a namespace, and a name.

The spec defines the details of how to connect to the external system, which is specific to each individual I/O module. Each identifier-spec pair is bound to the system inside an stateful function module.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
package org.apache.flink.statefun.docs.io.ingress;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public final class Identifiers {

    public static final IngressIdentifier<User> INGRESS =
        new IngressIdentifier<>(User.class, "example", "user-ingress");
}
{% endhighlight %}
{% highlight java %}
package org.apache.flink.statefun.docs.io.ingress;

import java.util.Map;
import org.apache.flink.statefun.docs.io.MissingImplementationException;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class ModuleWithIngress implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        IngressSpec<User> spec = createIngress(Identifiers.INGRESS);
        binder.bindIngress(spec);
    }

    private IngressSpec<User> createIngress(IngressIdentifier<User> identifier) {
        throw new MissingImplementationException("Replace with your specific ingress");
    }
}
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
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
{% endhighlight %}
</div>
</div>

## Router

A router is a stateless operator that takes each record from an ingress and routes it to zero or more functions.
Routers are bound to the system via a stateful function module, and unlike other components, an ingress may have any number of routers.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
package org.apache.flink.statefun.docs.io.ingress;

import org.apache.flink.statefun.docs.FnUser;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.Router;

public class UserRouter implements Router<User> {

    @Override
    public void route(User message, Downstream<User> downstream) {
        downstream.forward(FnUser.TYPE, message.getUserId(), message);
    }
}
{% endhighlight %}
{% highlight java %}
package org.apache.flink.statefun.docs.io.ingress;

import java.util.Map;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class ModuleWithRouter implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        IngressSpec<User> spec = createIngressSpec(Identifiers.INGRESS);
        Router<User> router = new UserRouter();

        binder.bindIngress(spec);
        binder.bindIngressRouter(Identifiers.INGRESS, router);
    }

    private IngressSpec<User> createIngressSpec(IngressIdentifier<User> identifier) {
        throw new MissingImplementationException("Replace with your specific ingress");
    }
}
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
When defined in ``yaml``, routers are defined by a list of function types.
The ``id`` component of the address is pulled from the key associated with each record in its underlying source implementation.
{% highlight yaml %}
targets:
    - example-namespace/my-function-1
    - example-namespace/my-function-2
{% endhighlight %}
</div>
</div>

## Egress

Egress is the opposite of ingress; it is a point that takes messages and writes them to external systems.
Each egress is defined using two components, an ``EgressIdentifier`` and an ``EgressSpec``.

An egress identifier uniquely identifies an egress based on a namespace, name, and producing type.
An egress spec defines the details of how to connect to the external system, the details are specific to each individual I/O module.
Each identifier-spec pair are bound to the system inside a stateful function module.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
package org.apache.flink.statefun.docs.io.egress;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

public final class Identifiers {

    public static final EgressIdentifier<User> EGRESS =
        new EgressIdentifier<>("example", "egress", User.class);
}

{% endhighlight %}
{% highlight java %}
package org.apache.flink.statefun.docs.io.egress;

import java.util.Map;
import org.apache.flink.statefun.docs.io.MissingImplementationException;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class ModuleWithEgress implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        EgressSpec<User> spec = createEgress(Identifiers.EGRESS);
        binder.bindEgress(spec);
    }

    public EgressSpec<User> createEgress(EgressIdentifier<User> identifier) {
        throw new MissingImplementationException("Replace with your specific egress");
    }
}
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
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
{% endhighlight %}
</div>
</div>

Stateful functions may then message an egress the same way they message another function, passing the egress identifier as function type.

{% highlight java %}
package org.apache.flink.statefun.docs.io.egress;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/** A simple function that outputs messages to an egress. */
public class FnOutputting implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
        context.send(Identifiers.EGRESS, new User());
    }
}
{% endhighlight %}