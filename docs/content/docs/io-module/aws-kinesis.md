---
title: AWS Kinesis
weight: 3
type: docs
aliases:
  - /io-module/aws-kinesis.html
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

# AWS Kinesis


Stateful Functions offers an AWS Kinesis I/O Module for reading from and writing to Kinesis streams.
It is based on Apache Flink's [Kinesis connector](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kinesis.html).
The Kinesis I/O Module is configurable in Yaml or Java.



## Dependency

{{< tabs "dep" >}}
{{< tab "Remote Module" >}}
If configuring an AWS Kinesis I/O Module as part of a remote module, there are no additional
dependencies to include in your application.
{{< /tab >}}
{{< tab "Embedded Module" >}}
To use the Kinesis I/O Module in Java, please include the following dependency in your pom.

{{< artifact statefun-kinesis-io withProvidedScope >}}

{{< /tab >}}
{{< /tabs >}}


## Kinesis Ingress Spec

A ``KinesisIngressSpec`` declares an ingress spec for consuming from Kinesis stream.

It accepts the following arguments:

1. The AWS region
2. An AWS credentials provider
3. A ``KinesisIngressDeserializer`` for deserializing data from Kinesis (Java only)
4. The stream start position
5. Properties for the Kinesis client
6. The name of the stream to consume from

{{< tabs "c1054004-8e8f-46db-90cc-faf57ef1f39d" >}}
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
                    typeUrl: com.googleapis/org.apache.flink.statefun.docs.models.User
                    targets:
                      - example-namespace/my-function-1
                      - example-namespace/my-function-2
                  - stream: stream-2
                    typeUrl: com.googleapis/org.apache.flink.statefun.docs.models.User
                    targets:
                      - example-namespace/my-function-1
                clientConfigProperties:
                  - SocketTimeout: 9999
                  - MaxConnections: 15
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
package org.apache.flink.statefun.docs.io.kinesis;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;

public class IngressSpecs {

  public static final IngressIdentifier<User> ID =
      new IngressIdentifier<>(User.class, "example", "input-ingress");

  public static final IngressSpec<User> kinesisIngress =
      KinesisIngressBuilder.forIdentifier(ID)
          .withAwsRegion("us-west-1")
          .withAwsCredentials(AwsCredentials.fromDefaultProviderChain())
          .withDeserializer(UserDeserializer.class)
          .withStream("stream-name")
          .withStartupPosition(KinesisIngressStartupPosition.fromEarliest())
          .withClientConfigurationProperty("key", "value")
          .build();
}
```
{{< /tab >}}
{{< /tabs >}}

The ingress also accepts properties to directly configure the Kinesis client, using ``KinesisIngressBuilder#withClientConfigurationProperty()``.
Please refer to the Kinesis [client configuration](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html) documentation for the full list of available properties.
Note that configuration passed using named methods will have higher precedence and overwrite their respective settings in the provided properties.

### Startup Position

The ingress allows configuring the startup position to be one of the following:

#### Latest (default)

Start consuming from the latest position, i.e. head of the stream shards.

{{< tabs "71944997-a4fb-4bfb-b6b2-42883b05f2b8" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: latest
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KinesisIngressStartupPosition#fromLatest();
```
{{< /tab >}}
{{< /tabs >}}

#### Earlist

Start consuming from the earliest position possible.

{{< tabs "c5f062ef-6570-48fa-9919-9533040aeead" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: earliest
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KinesisIngressStartupPosition#fromEarliest();
```
{{< /tab >}}
{{< /tabs >}}

#### Date

Starts from offsets that have an ingestion time larger than or equal to a specified date.

{{< tabs "a544f516-37d1-4f54-bf43-7170572891cd" >}}
{{< tab "Remote Module" >}}
```yaml
startupPosition:
    type: date
    date: 2020-02-01 04:15:00.00 Z
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
KinesisIngressStartupPosition#fromDate(ZonedDateTime.now());
```
{{< /tab >}}
{{< /tabs >}}

### Kinesis Deserializer

The Kinesis ingress needs to know how to turn the binary data in Kinesis into Java objects.
The ``KinesisIngressDeserializer`` allows users to specify such a schema.
The ``T deserialize(IngressRecord ingressRecord)`` method gets called for each Kinesis record, passing the binary data and metadata from Kinesis.

```java
package org.apache.flink.statefun.docs.io.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserDeserializer implements KinesisIngressDeserializer<User> {

  private static Logger LOG = LoggerFactory.getLogger(UserDeserializer.class);

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public User deserialize(IngressRecord ingressRecord) {
    try {
      return mapper.readValue(ingressRecord.getData(), User.class);
    } catch (IOException e) {
      LOG.debug("Failed to deserialize record", e);
      return null;
    }
  }
}
```

## Kinesis Egress Spec

A ``KinesisEgressBuilder`` declares an egress spec for writing data out to a Kinesis stream.

It accepts the following arguments:

1. The egress identifier associated with this egress
2. The AWS credentials provider
3. A ``KinesisEgressSerializer`` for serializing data into Kinesis (Java only)
4. The AWS region
5. Properties for the Kinesis client
6. The number of max outstanding records before backpressure is applied

{{< tabs "e9aa5f1d-5a32-41c9-b6e4-8f985e4cb481" >}}
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
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
package org.apache.flink.statefun.docs.io.kinesis;

import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;

public class EgressSpecs {

  public static final EgressIdentifier<User> ID =
      new EgressIdentifier<>("example", "output-egress", User.class);

  public static final EgressSpec<User> kinesisEgress =
      KinesisEgressBuilder.forIdentifier(ID)
          .withAwsCredentials(AwsCredentials.fromDefaultProviderChain())
          .withAwsRegion("us-west-1")
          .withMaxOutstandingRecords(100)
          .withClientConfigurationProperty("key", "value")
          .withSerializer(UserSerializer.class)
          .build();
}
```
{{< /tab >}}
{{< /tabs >}}

Please refer to the Kinesis [producer default configuration properties](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties) documentation for the full list of available properties.

### Kinesis Serializer

The Kinesis egress needs to know how to turn Java objects into binary data.
The ``KinesisEgressSerializer`` allows users to specify such a schema.
The ``EgressRecord serialize(T value)`` method gets called for each message, allowing users to set a value, and other metadata.

```java
package org.apache.flink.statefun.docs.io.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserSerializer implements KinesisEgressSerializer<User> {

  private static final Logger LOG = LoggerFactory.getLogger(UserSerializer.class);

  private static final String STREAM = "user-stream";

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public EgressRecord serialize(User value) {
    try {
      return EgressRecord.newBuilder()
          .withPartitionKey(value.getUserId())
          .withData(mapper.writeValueAsBytes(value))
          .withStream(STREAM)
          .build();
    } catch (IOException e) {
      LOG.info("Failed to serializer user", e);
      return null;
    }
  }
}
```

## AWS Region

Both the Kinesis ingress and egress can be configured to a specific AWS region.

#### Default Provider Chain (default)

Consults AWS's default provider chain to determine the AWS region.

{{< tabs "dfce86e5-df95-4d98-a74d-4e656c5eff01" >}}
{{< tab "Remote Module" >}}
```yaml
awsCredentials:
    type: default
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
AwsRegion.fromDefaultProviderChain();
```
{{< /tab >}}
{{< /tabs >}}

#### Specific

Specifies an AWS region using the region's unique id.

{{< tabs "ef7d29af-083e-4708-a9f2-62471ab8da50" >}}
{{< tab "Remote Module" >}}
```yaml
awsCredentials:
    type: specific
    id: us-west-1
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
AwsRegion.of("us-west-1");
```
{{< /tab >}}
{{< /tabs >}}


#### Custom Endpoint

Connects to an AWS region through a non-standard AWS service endpoint.
This is typically used only for development and testing purposes.

{{< tabs "450998b5-1c78-40ee-8626-c23945cee049" >}}
{{< tab "Remote Module" >}}
```yaml
awsCredentials:
    type: custom-endpoint
    endpoint: https://localhost:4567
    id: us-west-1
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
AwsRegion.ofCustomEndpoint("https://localhost:4567", "us-west-1");
```
{{< /tab >}}
{{< /tabs >}}

## AWS Credentials

Both the Kinesis ingress and egress can be configured using standard AWS credential providers.

#### Default Provider Chain (default)

Consults AWS’s default provider chain to determine the AWS credentials.

{{< tabs "eb8b18d5-c276-466c-9291-930b60071d55" >}}
{{< tab "Remote Module" >}}
```yaml
awsCredentials:
    type: default
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
AwsCredentials.fromDefaultProviderChain();
```
{{< /tab >}}
{{< /tabs >}}

#### Basic

Specifies the AWS credentials directly with provided access key ID and secret access key strings.

{{< tabs "0ac35336-1426-4fe1-89e4-f0eb02a6d4f4" >}}
{{< tab "Remote Module" >}}
```yaml
awsCredentials:
    type: basic
    accessKeyId: access-key-id
    secretAccessKey: secret-access-key
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
AwsCredentials.basic("accessKeyId", "secretAccessKey");
```
{{< /tab >}}
{{< /tabs >}}

#### Profile

Specifies the AWS credentials using an AWS configuration profile, along with the profile's configuration path.

{{< tabs "ac63d291-57de-4a16-ba61-d8d051b0a3ed" >}}
{{< tab "Remote Module" >}}
```yaml
awsCredentials:
    type: basic
    profileName: profile-name
    profilePath: /path/to/profile/config
```
{{< /tab >}}
{{< tab "Embedded Module" >}}
```java
AwsCredentials.profile("profile-name", "/path/to/profile/config");
```
{{< /tab >}}
{{< /tabs >}}

