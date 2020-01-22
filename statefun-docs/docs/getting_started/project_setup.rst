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

.. _project_setup:

#############
Project Setup
#############

You can quickly get started building a Stateful Functions applications by adding the ``statefun-sdk`` to an existing project or using the provided maven archetype.

.. toctree::
   :hidden:

Dependency
==========

.. code-block:: xml

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>statefun-sdk</artifactId>
            <version>{version}</version>
            <scope>provided</scope>
        </dependency>


Maven Archetype
===============

.. code-block:: bash

  $ mvn archetype:generate                    \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=statefun-quickstart \
    -DarchetypeVersion={version}

This allows you to name your newly created project.
It will interactively ask you for the groupId, artifactId, and package name.
There will be a new directory with the same name as your artifact id.

.. code-block:: bash

  $ tree statefun-quickstart/
  statefun-quickstart/
  ├── Dockerfile
  ├── pom.xml
  └── src
      └── main
          ├── java
          │   └── org
          │       └── apache
          |            └── flink
          │             └── statefun
          │              └── Module.java
          └── resources
              └── META-INF
                └── services
                  └── org.apache.flink.statefun.sdk.spi.StatefulFunctionModule

The project contains four files:

* ``pom.xml``: A pom file with the basic dependencies to start building a Stateful Functions application.
* ``Module``: The entry point for the application.
* ``org.apache.flink.statefun.sdk.spi.StatefulFunctionModule``: A service entry for the runtime to find the module.
* ``Dockerfile``: A Dockerfile to quickly build a Stateful Functions image ready to deploy.

We recommend you import this project into your IDE to develop and test it.
IntelliJ IDEA supports Maven projects out of the box.
If you use Eclipse, the m2e plugin allows to import Maven projects.
Some Eclipse bundles include that plugin by default, others require you to install it manually.

Build Project
=============

If you want to build/package your project, go to your project directory and run the ``mvn clean package`` command.
You will find a JAR file that contains your application, plus any libraries that you may have added as dependencies to the application: ``target/<artifact-id>-<version>.jar``.
