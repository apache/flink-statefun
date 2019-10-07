.. Copyright 2019 Ververica GmbH.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

########################
Packaging For Deployment
########################

Stateful Functions applications can be packaged as either standalone applications or Flink jobs that can be submitted to a cluster.

.. contents:: :local:

Images
^^^^^^

The recommended deployment mode for Stateful Functions applications is to build a Docker image.
This way, user code does not need to package any Apache Flink components.
The provided base image allows teams to package their applications with all the necessary runtime dependencies quickly.

Below is an example Dockerfile for building a Stateful Functions image for an application called ``stateful-functions-example``.

.. code-block:: java

    FROM stateful-functions

    RUN mkdir -p /opt/stateful-functions/modules/stateful-functions-example
    COPY target/stateful-functions-example*jar /opt/stateful-functions/modules/stateful-functions-example/

Flink Jar
^^^^^^^^^

If you prefer to package your job to submit to an existing Flink cluster, simply include ``stateful-functions-flink-distribution`` as a dependency to your application.

.. code-block:: xml

        <dependency>
            <groupId>com.ververica</groupId>
            <artifactId>stateful-functions-flink-distribution</artifactId>
            <version>{version}</version>
        </dependency>

It includes all of Stateful Functions' runtime dependencies and configures the application's main entry-point.
You do not need to take any action beyond adding the dependency to your POM.

.. note::

    The distribution must be bundled in your application fat JAR so that it is on Flink's `user code class loader <https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/debugging_classloading.html#inverted-class-loading-and-classloader-resolution-order>`_.

.. code-block:: bash

    ./bin/flink run ./stateful-functions-example.jar