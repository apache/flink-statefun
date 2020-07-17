---
title: Packaging For Deployment
nav-id: packaging
nav-pos: 1
nav-title: Packaging For Deployment
nav-parent_id: deployment-and-ops
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

Stateful Functions applications can be packaged as either standalone applications or Flink jobs that can be submitted to a cluster.

* This will be replaced by the TOC
{:toc}

## Images

The recommended deployment mode for Stateful Functions applications is to build a Docker image.
This way, user code does not need to package any Apache Flink components.
The provided base image allows teams to package their applications with all the necessary runtime dependencies quickly.

Below is an example Dockerfile for building a Stateful Functions image with both an [embedded module]({{ site.baseurl }}/sdk/modules.html#embedded-module) and a [remote module]({{ site.baseurl }}/sdk/modules.html#remote-module) for an application called ``statefun-example``.

{% highlight dockerfile %}
FROM flink-statefun:{{ site.version }}

RUN mkdir -p /opt/statefun/modules/statefun-example
RUN mkdir -p /opt/statefun/modules/remote

COPY target/statefun-example*jar /opt/statefun/modules/statefun-example/
COPY module.yaml /opt/statefun/modules/remote/module.yaml
{% endhighlight %}

{% if site.is_stable %}
<div class="alert alert-info">
	The Flink community is currently waiting for the official Docker images to be published to Docker Hub.
	In the meantime, Ververica has volunteered to make Stateful Functions' images available via their public registry: 

	<code class="language-dockerfile" data-lang="dockerfile">
		<span class="k">FROM</span><span class="s"> ververica/flink-statefun:{{ site.version }}</span>
	</code>

	You can follow the status of Docker Hub contribution <a href="https://github.com/docker-library/official-images/pull/7749">here</a>.
</div>
{% else %}
<div class="alert alert-info">
	<strong>Attention:</strong> The Flink community does not publish images for snapshot versions.
	You can build this version locally by cloning the <a hre="https://github.com/apache/flink-statefun">repo</a> and following
	the instructions in 
	
	<code class="language-dockerfile" data-lang="dockerfile">
		<span class="s">tools/docker/README.md</span>
	</code>
</div>
{% endif %}

## Flink Jar

If you prefer to package your job to submit to an existing Flink cluster, simply include ``statefun-flink-distribution`` as a dependency to your application.

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>statefun-flink-distribution</artifactId>
	<version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

It includes all of Stateful Functions' runtime dependencies and configures the application's main entry-point.

<div class="alert alert-info">
  <strong>Attention:</strong> The distribution must be bundled in your application fat JAR so that it is on Flink's <a href="https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/debugging_classloading.html#inverted-class-loading-and-classloader-resolution-order">user code class loader</a>
</div>

{% highlight bash %}
./bin/flink run -c org.apache.flink.statefun.flink.core.StatefulFunctionsJob ./statefun-example.jar
{% endhighlight %}

The following configurations are strictly required for running StateFun application.

{% highlight yaml %}
classloader.parent-first-patterns.additional: org.apache.flink.statefun;org.apache.kafka;com.google.protobuf
{% endhighlight %}

