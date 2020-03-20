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

###########
AWS Kinesis
###########

Stateful Functions offers an AWS Kinesis I/O Module for reading from and writing to Kinesis streams.
It is based on Apache Flink's `Kinesis connector <https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kinesis.html>`_.
The Kinesis I/O Module is configurable in Yaml or Java.

.. contents:: :local:

Dependency
==========

To use the Kinesis I/O Module in Java, please include the following dependency in your pom.

.. code-block:: xml

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>statefun-kinesis-io</artifactId>
        <version>{version}</version>
        <scope>provided</scope>
    </dependency>

Kinesis Ingress Spec
====================

A ``KinesisIngressSpec`` declares an ingress spec for consuming from Kinesis stream.

It accepts the following arguments:

1) The AWS region
2) An AWS credentials provider
3) A ``KinesisIngressDeserializer`` for deserializing data from Kinesis (Java only)
4) The stream start position
5) Properties for the Kinesis client
6) The name of the stream to consume from

.. tabs::

    .. group-tab:: Java 

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kinesis/IngressSpecs.java
            :language: java
            :lines: 18-

    .. group-tab:: Yaml

        .. code-block:: yaml 

            version: "1.0"

            module:
                meta:
                    type: remote
                spec:
                    ingresses:
                    - ingress:
                        meta:
                            type: statefun.kinesis.io/routable-protobuf-ingress
                            id: example-namespace/messages
                        spec:
                            awsRegion:
                                type: specific
                                id: us-west-1
                            awsCredentials:
                                type: basic
                                accessKeyId: my_access_key_id
                                secretAccessKey: my_secret_access_key
                            startupPosition:
                                type: earliest
                            streams:
                                - stream: stream-1
                                typeUrl: com.googleapis/com.mycomp.foo.Message
                                targets:
                                    - example-namespace/my-function-1
                                    - example-namespace/my-function-2
                                - stream: stream-2
                                    typeUrl: com.googleapis/com.mycomp.foo.Message
                                    targets:
                                        - example-namespace/my-function-1
                    clientConfigProperties:
                        - SocketTimeout: 9999
                        - MaxConnections: 15

The ingress also accepts properties to directly configure the Kinesis client, using ``KinesisIngressBuilder#withClientConfigurationProperty()``.
Please refer to the Kinesis `client configuration <https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html>`_ documentation for the full list of available properties.
Note that configuration passed using named methods will have higher precedence and overwrite their respective settings in the provided properties.

Startup Position
^^^^^^^^^^^^^^^^

The ingress allows configuring the startup position to be one of the following:

**Latest (default)**

Start consuming from the latest position, i.e. head of the stream shards.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KinesisIngressStartupPosition#fromLatest();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: latest

**Earliest**

Start consuming from the earliest position possible.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KinesisIngressStartupPosition#fromEarliest();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: earliest


**Date**

Starts from offsets that have an ingestion time larger than or equal to a specified date.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KinesisIngressStartupPosition#fromDate(ZonedDateTime.now())

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: date
                date: 2020-02-01 04:15:00.00 Z

Kinesis Deserializer
^^^^^^^^^^^^^^^^^^^^

The Kinesis ingress needs to know how to turn the binary data in Kinesis into Java objects.
The ``KinesisIngressDeserializer`` allows users to specify such a schema.
The ``T deserialize(IngressRecord ingressRecord)`` method gets called for each Kinesis record, passing the binary data and metadata from Kinesis.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kinesis/UserDeserializer.java
    :language: java
    :lines: 18-

Kinesis Egress Spec
===================

A ``KinesisEgressBuilder`` declares an egress spec for writing data out to a Kinesis stream.

It accepts the following arguments:

1) The egress identifier associated with this egress
2) The AWS credentials provider
3) A ``KinesisEgressSerializer`` for serializing data into Kinesis (Java only)
4) The AWS region
5) Properties for the Kinesis client
6) The number of max outstanding records before backpressure is applied

.. tabs::

    .. group-tab:: Java 

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kinesis/EgressSpecs.java
            :language: java
            :lines: 18-

    .. group-tab:: Yaml

        .. code-block:: yaml 

            version: "1.0"

            module:
                meta:
                    type: remote
                spec:
                    egresses:
                    - egress:
                        meta:
                            type: statefun.kinesis.io/generic-egress
                            id: example/output-messages
                        spec:
                            awsRegion:
                                type: custom-endpoint
                                endpoint: https://localhost:4567
                                id: us-west-1
                            awsCredentials:
                                type: profile
                                profileName: john-doe
                                profilePath: /path/to/profile/config
                             maxOutstandingRecords: 9999
                             clientConfigProperties:
                                - ThreadingModel: POOLED
                                - ThreadPoolSize: 10

Please refer to the Kinesis `client configuration <https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html>`_ documentation for the full list of available properties.

Kinesis Serializer
^^^^^^^^^^^^^^^^^^

The Kinesis egress needs to know how to turn Java objects into binary data.
The ``KinesisEgressSerializer`` allows users to specify such a schema.
The ``EgressRecord serialize(T value)`` method gets called for each message, allowing users to set a value, and other metadata.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kinesis/UserSerializer.java
    :language: java
    :lines: 18-

AWS Credentials
===============

Both the Kinesis ingress and egress can be configured using standard AWS credential providers.

**Default Provider Chain (default)**

Consults AWS’s default provider chain to determine the AWS credentials.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            AwsCredentials.fromDefaultProviderChain();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            awsCredentials:
                type: default

**Basic**

Specifies the AWS credentials directly with provided access key ID and secret access key strings.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            AwsCredentials.basic("accessKeyId", "secretAccessKey");

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            awsCredentials:
                type: basic
                accessKeyId: acess-key-id
                secretAccessKey: secret-access-key 

**Profile**

Specifies the AWS credentials using an AWS configuration profile, along with the profile's configuration path.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            AwsCredentials.profile("profile-name", "/path/to/profile/config");

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            awsCredentials:
                type: profile
                profileName: profile-name
                profilePath: /path/to/profile/config

AWS Region
==========

Both the Kinesis ingress and egress can be configured to a specific AWS region.

**Default Provider Chain (default)**

Consults AWS's default provider chain to determine the AWS region.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            AwsRegion.fromDefaultProviderChain();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            awsRegion:
                type: default

**Specific**

Specifies an AWS region using the region's unique id.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            AwsRegion.of("us-west-1");

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            awsRegion:
                type: specific
                id: us-west-1

**Custom Endpoint**

Connects to an AWS region through a non-standard AWS service endpoint.
This is typically used only for development and testing purposes.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            AwsRegion.ofCustomEndpoint("https://localhost:4567", "us-west-1");

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            awsRegion:
                type: custom-endpoint
                endpoint: https://localhost:4567
                id: us-west-1
