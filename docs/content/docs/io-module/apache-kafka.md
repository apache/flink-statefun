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
Kafka is configured in the [module specification]({{< ref "docs/deployment/module" >}}) of your application.

## Kafka Ingress Spec

A Kafka ingress defines an input point that reads records from one or more topics.

```yaml
version: "3.0"

module:
  meta:
    type: remote
spec:
  ingresses:
  - ingress:
      meta:
        type: io.statefun.kafka/ingress
        id: com.example/users
      spec:
        address: kafka-broker:9092
        consumerGroupId: my-consumer-group
        startupPosition:
          type: earliest
        topics:
          - topic: messages-1
            valueType: com.example/User
            targets:
              - com.example.fns/greeter
```

The ingress also accepts properties to directly configure the Kafka client, using ``ingress.spec.properties``.
Please refer to the Kafka [consumer configuration](https://docs.confluent.io/current/installation/configuration/consumer-configs.html) documentation for the full list of available properties.
Note that configuration passed using named paths, such as ``ingress.spec.address``, will have higher precedence and overwrite their respective settings in the provided properties.

### Startup Position

The ingress allows configuring the startup position to be one of the following:

#### From Group Offset (default)

Starts from offsets that were committed to Kafka for the specified consumer group.

```yaml
startupPosition:
  type: group-offsets
```

#### Earliest

Starts from the earliest offset.

```yaml
startupPosition:
  type: earliest
```

#### Latest

Starts from the latest offset.

```yaml
startupPosition:
  type: latest
```

#### Specific Offsets

Starts from specific offsets, defined as a map of partitions to their target starting offset.

```yaml
startupPosition:
  type: specific-offsets
  offsets:
    - user-topic/0: 91
    - user-topic/1: 11
    - user-topic/2: 8
```

#### Date

Starts from offsets that have an ingestion time larger than or equal to a specified date.

```yaml
startupPosition:
  type: date
  date: 2020-02-01 04:15:00.00 Z
```

On startup, if the specified startup offset for a partition is out-of-range or does not exist (which may be the case if the ingress is configured to start from group offsets, specific offsets, or from a date), then the ingress will fallback to using the position configured using ``ingress.spec.autoOffsetResetPosition`` which may be set to either `latest` or `earliest`.
By default, this is set to be the `latest` position.


## Kafka Egress Spec

A Kafka egress defines an input point where functions can write out records to one or more topics.

```yaml
version: "3.0"

module:
  meta:
    type: remote
spec:
  egresses:
    - egress:
        meta:
          type: io.statefun.kafka/egress
          id: example/output-messages
       spec:
         address: kafka-broker:9092
         deliverySemantic:
           type: exactly-once
           transactionTimeout: 15min
         properties:
           - foo.config: bar
```

Please refer to the Kafka [producer configuration](https://docs.confluent.io/current/installation/configuration/producer-configs.html) documentation for the full list of available properties.

### Kafka Egress and Fault Tolerance

With fault tolerance enabled, the Kafka egress can provide exactly-once delivery guarantees.
You can choose three different modes of operation.

#### None

Nothing is guaranteed, produced records can be lost or duplicated.

```yaml
deliverySemantic:
  type: none
```

#### At Least Once

Stateful Functions will guarantee that no records will be lost but they can be duplicated.

```yaml
deliverySemantic:
  type: at-least-once
```

#### Exactly Once

Stateful Functions uses Kafka transactions to provide exactly-once semantics.

```yaml
deliverySemantic:
  type: exactly-once
  transactionTimeoutMillis: 15min
```

### Writing To Kafka

Functions write directly to Kafka from their SDK context.
See SDK specific documentation for more details.
