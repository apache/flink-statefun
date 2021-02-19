---
title: Apache Kafka
weight: 2
type: docs
aliases:
  - /io-module/apache-kafka.html
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

# Apache Kafka


Stateful Functions offers an Apache Kafka I/O Module for reading from and writing to Kafka topics.
It is based on Apache Flink's universal [Kafka connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html) and provides exactly-once processing semantics.
The Kafka I/O Module is configurable in Yaml or Java.



## Dependency

{{< tabs "dep" >}}
{{< tab "Remote Module" >}}
If configuring an Apache Kafka I/O Module as part of a remote module, there are no additional
dependencies to include in your application.
{{< /tab >}}
{{< tab "Embedded Module" >}}
To use the Kafka I/O Module in Java, please include the following dependency in your pom.

{{< artifact statefun-kafka-io withProvidedScope >}}

{{< /tab >}}
{{< /tabs >}}

## Kafka Ingress Spec

A ``KafkaIngressSpec`` declares an ingress spec for consuming from Kafka cluster.

It accepts the following arguments:

1. The ingress identifier associated with this ingress
2. The topic name / list of topic names
3. The address of the bootstrap servers
4. The consumer group id to use
5. A ``KafkaIngressDeserializer`` for deserializing data from Kafka (Java only)
6. The position to start consuming from

{{< tabs "7cf3a0a2-608e-4af9-a5db-e754d85450c0" >}}
{{< tab "Remote Module" >}}
```yaml
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
                typeUrl: org.apache.flink.statefun.docs.models.User
                targets:
                  - example-namespace/my-function-1
                  - example-namespace/my-function-2
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
package org.apache.flink.statefun.docs.io.kafka;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;

public class IngressSpecs {

  public static final IngressIdentifier<User> ID =
      new IngressIdentifier<>(User.class, "example", "input-ingress");

  public static final IngressSpec<User> kafkaIngress =
      KafkaIngressBuilder.forIdentifier(ID)
          .withKafkaAddress("localhost:9092")
          .withConsumerGroupId("greetings")
          .withTopic("my-topic")
          .withDeserializer(UserDeserializer.class)
          .withStartupPosition(KafkaIngressStartupPosition.fromLatest())
          .build();
}
```
{{< /tab >}}
{{< /tabs >}}

The ingress also accepts properties to directly configure the Kafka client, using ``KafkaIngressBuilder#withProperties(Properties)``.
Please refer to the Kafka [consumer configuration](https://docs.confluent.io/current/installation/configuration/consumer-configs.html) documentation for the full list of available properties.
Note that configuration passed using named methods, such as ``KafkaIngressBuilder#withConsumerGroupId(String)``, will have higher precedence and overwrite their respective settings in the provided properties.

### Startup Position

The ingress allows configuring the startup position to be one of the following:

#### From Group Offset (default)

Starts from offsets that were committed to Kafka for the specified consumer group.

{{< tabs "e87cd357-97c2-4364-bf93-4d1d9c65a6a6" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: group-offsets
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaIngressStartupPosition#fromGroupOffsets();
```
{{< /tab >}}
{{< /tabs >}}

#### Earlist

Starts from the earliest offset.

{{< tabs "8609e959-9c92-4d33-9e38-df8496c7b3f5" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: earliest
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaIngressStartupPosition#fromEarliest();
```
{{< /tab >}}
{{< /tabs >}}

#### Latest

Starts from the latest offset.

{{< tabs "f464e8de-fc98-4926-98d8-2a7afcf6c5b7" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: latest
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaIngressStartupPosition#fromLatest();
```
{{< /tab >}}
{{< /tabs >}}

#### Specific Offsets

Starts from specific offsets, defined as a map of partitions to their target starting offset.

{{< tabs "b1ec723b-a999-4e64-9c72-0c08678799f3" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: specific-offsets
    offsets:
        - user-topic/0: 91
        - user-topic/1: 11
        - user-topic/2: 8
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
Map<TopicPartition, Long> offsets = new HashMap<>();
offsets.add(new TopicPartition("user-topic", 0), 91);
offsets.add(new TopicPartition("user-topic", 11), 11);
offsets.add(new TopicPartition("user-topic", 8), 8);

KafkaIngressStartupPosition#fromSpecificOffsets(offsets);
```
{{< /tab >}}
{{< /tabs >}}

#### Date

Starts from offsets that have an ingestion time larger than or equal to a specified date.

{{< tabs "1d3bd4cf-144b-4eaa-976a-0c58a587d46b" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: date
    date: 2020-02-01 04:15:00.00 Z
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaIngressStartupPosition#fromDate(ZonedDateTime.now());
```
{{< /tab >}}
{{< /tabs >}}

On startup, if the specified startup offset for a partition is out-of-range or does not exist (which may be the case if the ingress is configured to start from group offsets, specific offsets, or from a date), then the ingress will fallback to using the position configured using ``KafkaIngressBuilder#withAutoOffsetResetPosition(KafkaIngressAutoResetPosition)``.
By default, this is set to be the latest position.

### Kafka Deserializer

When using the Java api, the Kafka ingress needs to know how to turn the binary data in Kafka into Java objects.
The ``KafkaIngressDeserializer`` allows users to specify such a schema.
The ``T deserialize(ConsumerRecord<byte[], byte[]> record)`` method gets called for each Kafka message, passing the key, value, and metadata from Kafka.

```java
package org.apache.flink.statefun.docs.io.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserDeserializer implements KafkaIngressDeserializer<User> {

	private static Logger LOG = LoggerFactory.getLogger(UserDeserializer.class);

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public User deserialize(ConsumerRecord<byte[], byte[]> input) {
		try {
			return mapper.readValue(input.value(), User.class);
		} catch (IOException e) {
			LOG.debug("Failed to deserialize record", e);
			return null;
		}
	}
}
```

## Kafka Egress Spec

A ``KafkaEgressBuilder`` declares an egress spec for writing data out to a Kafka cluster.

It accepts the following arguments:

1. The egress identifier associated with this egress
2. The address of the bootstrap servers
3. A ``KafkaEgressSerializer`` for serializing data into Kafka (Java only)
4. The fault tolerance semantic
5. Properties for the Kafka producer

{{< tabs "62444f98-c036-4b2c-a2b2-4f187542f37f" >}}
{{< tab "Remote Module" >}}
```yaml
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
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
package org.apache.flink.statefun.docs.io.kafka;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;

public class EgressSpecs {

  public static final EgressIdentifier<User> ID =
      new EgressIdentifier<>("example", "output-egress", User.class);

  public static final EgressSpec<User> kafkaEgress =
      KafkaEgressBuilder.forIdentifier(ID)
          .withKafkaAddress("localhost:9092")
          .withSerializer(UserSerializer.class)
          .build();
}
```
{{< /tab >}}
{{< /tabs >}}

Please refer to the Kafka [producer configuration](https://docs.confluent.io/current/installation/configuration/producer-configs.html) documentation for the full list of available properties.

### Kafka Egress and Fault Tolerance

With fault tolerance enabled, the Kafka egress can provide exactly-once delivery guarantees.
You can choose three different modes of operation.

#### None

Nothing is guaranteed, produced records can be lost or duplicated.

{{< tabs "3da2f1be-871a-4ae1-8cc0-d04cccee26de" >}}
{{< tab "Remote Module" >}}
```yaml
deliverySemantic:
    type: none
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaEgressBuilder#withNoProducerSemantics();
```
{{< /tab >}}
{{< /tabs >}}

#### At Least Once

Stateful Functions will guarantee that no records will be lost but they can be duplicated.

{{< tabs "a50f0607-3ec8-4161-9a1f-2f65eb267f4f" >}}
{{< tab "Remote Module" >}}
```yaml
deliverySemantic:
    type: at-least-once
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaEgressBuilder#withAtLeastOnceProducerSemantics();
```
{{< /tab >}}
{{< /tabs >}}

#### Exactly Once

Stateful Functions uses Kafka transactions to provide exactly-once semantics.

{{< tabs "5e34d5c3-ffdd-452c-9a5b-b4e180afe70d" >}}
{{< tab "Remote Module" >}}
```yaml
deliverySemantic:
    type: exactly-once
    transactionTimeoutMillis: 900000 # 15 min
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KafkaEgressBuilder#withExactlyOnceProducerSemantics(Duration.minutes(15));
```
{{< /tab >}}
{{< /tabs >}}

### Kafka Serializer

When using the Java api, the Kafka egress needs to know how to turn Java objects into binary data.
The ``KafkaEgressSerializer`` allows users to specify such a schema.
The ``ProducerRecord<byte[], byte[]> serialize(T out)`` method gets called for each message, allowing users to set a key, value, and other metadata.

```java
package org.apache.flink.statefun.docs.io.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserSerializer implements KafkaEgressSerializer<User> {

  private static final Logger LOG = LoggerFactory.getLogger(UserSerializer.class);

  private static final String TOPIC = "user-topic";

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ProducerRecord<byte[], byte[]> serialize(User user) {
    try {
      byte[] key = user.getUserId().getBytes();
      byte[] value = mapper.writeValueAsBytes(user);

      return new ProducerRecord<>(TOPIC, key, value);
    } catch (JsonProcessingException e) {
      LOG.info("Failed to serializer user", e);
      return null;
    }
  }
}
```