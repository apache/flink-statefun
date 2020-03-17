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

############
Apache Kafka
############

Stateful Functions offers an Apache Kafka I/O Module for reading from and writing to Kafka topics.
It is based on Apache Flink's universal `Kafka connector <https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html>`_ and provides exactly-once processing semantics.
The Kafka I/O Module is configurable in Yaml or Java.

.. contents:: :local:

Dependency
==========

To use the Kafka I/O Module in Java, please include the following dependency in your pom.

.. code-block:: xml

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>statefun-kafka-io</artifactId>
        <version>{version}</version>
        <scope>provided</scope>
    </dependency>

Kafka Ingress Spec
==================

A ``KafkaIngressSpec`` declares an ingress spec for consuming from Kafka cluster.

It accepts the following arguments: 

1) The ingress identifier associated with this ingress
2) The topic name / list of topic names
3) The address of the bootstrap servers
4) The consumer group id to use
5) A ``KafkaIngressDeserializer`` for deserializing data from Kafka (Java only)
6) The position to start consuming from

.. tabs:: 

    .. group-tab:: Java

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kafka/IngressSpecs.java
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
                        type: statefun.kafka.io/routable-protobuf-ingress
                        id: example/user-ingress
                    spec:
                        address: kafka-broker:9092
                        consumerGroupId: routable-kafka-e2e
                        startupPosition:
                            type: earliest
                        topics:
                          - topic: messages-1
                            typeUrl: com.googleapis/com.company.MessageWithAddress
                            targets:
                              - example-namespace/my-function-1
                              - example-namespace/my-function-2

The ingress also accepts properties to directly configure the Kafka client, using ``KafkaIngressBuilder#withProperties(Properties)``.
Please refer to the Kafka `consumer configuration <https://docs.confluent.io/current/installation/configuration/consumer-configs.html>`_ documentation for the full list of available properties.
Note that configuration passed using named methods, such as ``KafkaIngressBuilder#withConsumerGroupId(String)``, will have higher precedence and overwrite their respective settings in the provided properties.

Startup Position
^^^^^^^^^^^^^^^^

The ingress allows configuring the startup position to be one of the following:

**From Group Offset (default)**

Starts from offsets that were committed to Kafka for the specified consumer group.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaIngressStartupPosition#fromGroupOffsets();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: group-offsets

**Earliest** 

Starts from the earliest offset.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaIngressStartupPosition#fromEarliest();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: earliest

**Latest**

Starts from the latest offset.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaIngressStartupPosition#fromLatest();

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: latest

**Specific Offsets**

Starts from specific offsets, defined as a map of partitions to their target starting offset.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.add(new TopicPartition("user-topic", 0), 91);
            offsets.add(new TopicPartition("user-topic", 11), 11);
            offsets.add(new TopicPartition("user-topic", 8), 8);

            KafkaIngressStartupPosition#fromSpecificOffsets(offsets);

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: specific-offsets
                offsets:
                    - user-topic/0: 91
                    - user-topic/1: 11
                    - user-topic/2: 8

**Date**

Starts from offsets that have an ingestion time larger than or equal to a specified date.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaIngressStartupPosition#fromDate(ZonedDateTime.now());

    .. group-tab:: Yaml 

        .. code-block:: yaml 

            startupPosition:
                type: date
                date: 2020-02-01 04:15:00.00 Z

On startup, if the specified startup offset for a partition is out-of-range or does not exist (which may be the case if the ingress is configured to
start from group offsets, specific offsets, or from a date), then the ingress will fallback to using the position configured
using ``KafkaIngressBuilder#withAutoOffsetResetPosition(KafkaIngressAutoResetPosition)``.
By default, this is set to be the latest position.

Kafka Deserializer
^^^^^^^^^^^^^^^^^^

When using the Java api, the Kafka ingress needs to know how to turn the binary data in Kafka into Java objects.
The ``KafkaIngressDeserializer`` allows users to specify such a schema.
The ``T deserialize(ConsumerRecord<byte[], byte[]> record)`` method gets called for each Kafka message, passing the key, value, and metadata from Kafka.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kafka/UserDeserializer.java
    :language: java
    :lines: 18-

Kafka Egress Spec
=================

A ``KafkaEgressBuilder`` declares an egress spec for writing data out to a Kafka cluster.

It accepts the following arguments: 

1) The egress identifier associated with this egress
2) The address of the bootstrap servers
3) A ``KafkaEgressSerializer`` for serializing data into Kafka (Java only)
4) The fault tolerance semantic
5) Properties for the Kafka producer

.. tabs::

    .. group-tab:: Java 

        .. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kafka/EgressSpecs.java
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
                        type: statefun.kafka.io/generic-egress
                        id: example/output-messages
                      spec:
                        address: kafka-broker:9092
                        deliverySemantic:
                          type: exactly-once
                          transactionTimeoutMillis: 100000
                        properties:
                          - foo.config: bar

Please refer to the Kafka `producer configuration <https://docs.confluent.io/current/installation/configuration/producer-configs.html>`_ documentation for the full list of available properties.

Kafka Egress and Fault Tolerance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With fault tolerance enabled, the Kafka egress can provide exactly-once delivery guarantees.
You can choose three different modes of operation.

**None**

Nothing is guaranteed, produced records can be lost or duplicated.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaEgressBuilder#withNoProducerSemantics();

    .. group-tab:: Yaml

        .. code-block:: yaml 

            deliverySemantic:
                type: none

**At Least Once**

Stateful Functions will guarantee that no records will be lost but they can be duplicated.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaEgressBuilder#withAtLeastOnceProducerSemantics();

    .. group-tab:: Yaml

        .. code-block:: yaml 

            deliverySemantic:
                type: at-least-once

**Exactly Once**

Stateful Functions uses Kafka transactions to provide exactly-once semantics.

.. tabs::

    .. group-tab:: Java

        .. code-block:: none

            KafkaEgressBuilder#withExactlyOnceProducerSemantics(Duration.minutes(15));

    .. group-tab:: Yaml

        .. code-block:: yaml 

            deliverySemantic:
                type: exactly-once
                transactionTimeoutMillis: 900000 # 15 min 

Kafka Serializer
^^^^^^^^^^^^^^^^

When using the Java api, the Kafka egress needs to know how to turn Java objects into binary data.
The ``KafkaEgressSerializer`` allows users to specify such a schema.
The ``ProducerRecord<byte[], byte[]> serialize(T out)`` method gets called for each message, allowing users to set a key, value, and other metadata.

.. literalinclude:: ../../src/main/java/org/apache/flink/statefun/docs/io/kafka/UserSerializer.java
    :language: java
    :lines: 18-
