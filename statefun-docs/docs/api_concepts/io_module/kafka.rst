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

.. contents:: :local:

Dependency
===========

To use the Kafka I/O Module, please include the following dependency in your pom.

.. code-block:: xml

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>statefun-kafka-io</artifactId>
        <version>{version}</version>
        <scope>provided</scope>
    </dependency>

Kafka Ingress Builder
=====================

A ``KafkaIngressBuilder`` declares an ingress spec for consuming from Kafka cluster. 

It accepts the following arguments: 

1) The ingress identifier associated with this ingress
2) The topic name / list of topic names
3) The address of the bootstrap servers
4) The consumer group id to use
5) A ``KafkaIngressDeserializer`` for deserializing data from Kafka
6) The position to start consuming from

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/kafka/IngressSpecs.java
    :language: java
    :lines: 18-

The ingress allows configuring the startup position to be one of the following:

* ``KafkaIngressStartupPosition#fromGroupOffsets()`` (default): starts from offsets that were committed to Kafka for the specified consumer group.
* ``KafkaIngressStartupPosition#fromEarliest()``: starts from the earliest offset.
* ``KafkaIngressStartupPosition#fromLatest()``: starts from the latest offset.
* ``KafkaIngressStartupPosition#fromSpecificOffsets(Map)``: starts from specific offsets, defined as a map of partitions to their target starting offset.
* ``KafkaIngressStartupPosition#fromDate(Date)``: starts from offsets that have an ingestion time larger than or equal to a specified date.

On startup, if the specified startup offset for a partition is out-of-range or does not exist (which may be the case if the ingress is configured to
start from group offsets, specific offsets, or from a date), then the ingress will fallback to using the position configured
using ``KafkaIngressBuilder#withAutoOffsetResetPosition(KafkaIngressAutoResetPosition)``. By default, this is set to be the latest position.

The ingress also accepts properties to directly configure the Kafka client, using ``KafkaIngressBuilder#withProperties(Properties)``.
Please refer to the Kafka `consumer configuration <https://docs.confluent.io/current/installation/configuration/consumer-configs.html>`_ documentation for the full list of available properties.
Note that configuration passed using named methods, such as ``KafkaIngressBuilder#withConsumerGroupId(String)``, will have higher precedence and overwrite their respective settings in the provided properties.

Kafka Deserializer
""""""""""""""""""

The Kafka ingress needs to know how to turn the binary data in Kafka into Java objects.
The ``KafkaIngressDeserializer`` allows users to specify such a schema.
The ``T deserialize(ConsumerRecord<byte[], byte[]> record)`` method gets called for each Kafka message, passing the key, value, and metadata from Kafka.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/kafka/UserDeserializer.java
    :language: java
    :lines: 18-

Kafka Egress Spec
=================

A ``KafkaEgressBuilder`` declares an egress spec for writing data out to a Kafka cluster.

It accepts the following arguments: 

1) The egress identifier associated with this egress
2) The address of the bootstrap servers
3) A ``KafkaEgressSerializer`` for serializing data into Kafka
4) The fault tolerance semantic
5) Properties for the Kafka producer

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/kafka/EgressSpecs.java
    :language: java
    :lines: 18-

Please refer to the Kafka `producer configuration <https://docs.confluent.io/current/installation/configuration/producer-configs.html>`_ documentation for the full list of available properties.

Kafka Egress and Fault Tolerance
""""""""""""""""""""""""""""""""

With fault tolerance enabled, the Kafka egress can provide exactly-once delivery guarantees.
You can choose three different modes of operating based through the ``KafkaEgressBuilder``.

* ``KafkaEgressBuilder#withNoProducerSemantics``: Nothing is guaranteed. Produced records can be lost or duplicated.
* ``KafkaEgressBuilder#withAtLeastOnceProducerSemantics``: Stateful Functions will guarantee that nor records will be lost but they can be duplicated.
* ``KafkaEgressBuilder#withExactlyOnceProducerSemantics``: Stateful Functions uses Kafka transactions to provide exactly-once semantics.

Kafka Serializer
""""""""""""""""

The Kafka egress needs to know how to turn Java objects into binary data.
The ``KafkaEgressSerializer`` allows users to specify such a schema.
The ``ProducerRecord<byte[], byte[]> serialize(T out)`` method gets called for each message, allowing users to set a key, value, and other metadata.

.. literalinclude:: ../../../src/main/java/org/apache/flink/statefun/docs/io/kafka/UserSerializer.java
    :language: java
    :lines: 18-
