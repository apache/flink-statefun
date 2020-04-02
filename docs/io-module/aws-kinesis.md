---
title: AWS Kinesis
nav-id: aws-kinesis
nav-pos: 2
nav-title: AWS Kinesis
nav-parent_id: io-module
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


Stateful Functions offers an AWS Kinesis I/O Module for reading from and writing to Kinesis streams.
It is based on Apache Flink's [Kinesis connector](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kinesis.html).
The Kinesis I/O Module is configurable in Yaml or Java.

* This will be replaced by the TOC
{:toc}

## Dependency

To use the Kinesis I/O Module in Java, please include the following dependency in your pom.

{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>statefun-kinesis-io</artifactId>
    <version>{{ site.version }}</version>
    <scope>provided</scope>
</dependency>
{% endhighlight %}

## Kinesis Ingress Spec

A ``KinesisIngressSpec`` declares an ingress spec for consuming from Kinesis stream.

It accepts the following arguments:

1. The AWS region
2. An AWS credentials provider
3. A ``KinesisIngressDeserializer`` for deserializing data from Kinesis (Java only)
4. The stream start position
5. Properties for the Kinesis client
6. The name of the stream to consume from

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>
</div>

The ingress also accepts properties to directly configure the Kinesis client, using ``KinesisIngressBuilder#withClientConfigurationProperty()``.
Please refer to the Kinesis [client configuration](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html) documentation for the full list of available properties.
Note that configuration passed using named methods will have higher precedence and overwrite their respective settings in the provided properties.

### Startup Position

The ingress allows configuring the startup position to be one of the following:

#### Latest (default)

Start consuming from the latest position, i.e. head of the stream shards.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
KinesisIngressStartupPosition#fromLatest();
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
startupPosition:
    type: latest
{% endhighlight %}
</div>
</div>

#### Earlist

Start consuming from the earliest position possible.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
KinesisIngressStartupPosition#fromEarliest();
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
startupPosition:
    type: earliest
{% endhighlight %}
</div>
</div>

#### Date

Starts from offsets that have an ingestion time larger than or equal to a specified date.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
KinesisIngressStartupPosition#fromDate(ZonedDateTime.now());
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
startupPosition:
    type: date
    date: 2020-02-01 04:15:00.00 Z
{% endhighlight %}
</div>
</div>

### Kinesis Deserializer

The Kinesis ingress needs to know how to turn the binary data in Kinesis into Java objects.
The ``KinesisIngressDeserializer`` allows users to specify such a schema.
The ``T deserialize(IngressRecord ingressRecord)`` method gets called for each Kinesis record, passing the binary data and metadata from Kinesis.

{% highlight java %}
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
{% endhighlight %}

## Kinesis Egress Spec

A ``KinesisEgressBuilder`` declares an egress spec for writing data out to a Kinesis stream.

It accepts the following arguments:

1. The egress identifier associated with this egress
2. The AWS credentials provider
3. A ``KinesisEgressSerializer`` for serializing data into Kinesis (Java only)
4. The AWS region
5. Properties for the Kinesis client
6. The number of max outstanding records before backpressure is applied

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>
</div>

Please refer to the Kinesis [producer default configuration properties](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties) documentation for the full list of available properties.

### Kinesis Serializer

The Kinesis egress needs to know how to turn Java objects into binary data.
The ``KinesisEgressSerializer`` allows users to specify such a schema.
The ``EgressRecord serialize(T value)`` method gets called for each message, allowing users to set a value, and other metadata.

{% highlight java %}
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
{% endhighlight %}

## AWS Region

Both the Kinesis ingress and egress can be configured to a specific AWS region.

#### Default Provider Chain (default)

Consults AWS's default provider chain to determine the AWS region.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
AwsRegion.fromDefaultProviderChain();
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
awsCredentials:
    type: default
{% endhighlight %}
</div>
</div>

#### Specific

Specifies an AWS region using the region's unique id.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
AwsRegion.of("us-west-1");
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
awsCredentials:
    type: specific
    id: us-west-1
{% endhighlight %}
</div>
</div>


#### Custom Endpoint

Connects to an AWS region through a non-standard AWS service endpoint.
This is typically used only for development and testing purposes.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
AwsRegion.ofCustomEndpoint("https://localhost:4567", "us-west-1");
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
awsCredentials:
    type: custom-endpoint
    endpoint: https://localhost:4567
    id: us-west-1
{% endhighlight %}
</div>
</div>

## AWS Credentials

Both the Kinesis ingress and egress can be configured using standard AWS credential providers.

#### Default Provider Chain (default)

Consults AWS’s default provider chain to determine the AWS credentials.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
AwsCredentials.fromDefaultProviderChain();
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
awsCredentials:
    type: default
{% endhighlight %}
</div>
</div>

#### Basic

Specifies the AWS credentials directly with provided access key ID and secret access key strings.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
AwsCredentials.basic("accessKeyId", "secretAccessKey");
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
awsCredentials:
    type: basic
    accessKeyId: access-key-id
    secretAccessKey: secret-access-key
{% endhighlight %}
</div>
</div>

#### Profile

Specifies the AWS credentials using an AWS configuration profile, along with the profile's configuration path.

<div class="codetabs" markdown="1">
<div data-lang="Embedded Module" markdown="1">
{% highlight java %}
AwsCredentials.profile("profile-name", "/path/to/profile/config");
{% endhighlight %}
</div>
<div data-lang="Remote Module" markdown="1">
{% highlight yaml %}
awsCredentials:
    type: basic
    profileName: profile-name
    profilePath: /path/to/profile/config
{% endhighlight %}
</div>
</div>

