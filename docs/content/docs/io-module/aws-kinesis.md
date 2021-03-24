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
Kinesis is configured in the [module specification]({{< ref "docs/deployment/module" >}}) of your application.


## Kinesis Ingress Spec

A Kinesis ingress defines an input point that reads records from one or more streams.

```yaml
version: "3.0"

module:
  meta:
    type: remote
  spec:
    ingresses:
      - ingress:
          meta:
            type: io.statefun.kinesis/ingress
            id: com.example/users
          spec:
            awsRegion:
              type: specific
              id: eu-west-1
            startupPosition:
              type: latest
            streams:
              - stream: user-stream
                typeUrl: com.example/User
                targets:
                  - com.example.fn/greeter
            clientConfigProperties:
              - SocketTimeout: 9999
              - MaxConnections: 15
                type: statefun.kinesis.io/routable-protobuf-ingress
                id: example-namespace/messages
```

Please refer to the Kinesis [client configuration](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html) documentation for the full list of available properties.
Note that configuration passed using named methods will have higher precedence and overwrite their respective settings in the provided properties.

### Startup Position

The ingress allows configuring the startup position to be one of the following:

#### Latest (default)

Start consuming from the latest position, i.e. head of the stream shards.

```yaml
startupPosition:
  type: latest
```

#### Earlist

Start consuming from the earliest position possible.

```yaml
startupPosition:
  type: earliest
```

#### Date

Starts from offsets that have an ingestion time larger than or equal to a specified date.

```yaml
startupPosition:
  type: date
  date: 2020-02-01 04:15:00.00 Z
```

## Kinesis Egress Spec

A Kinesis egress defines an input point where functions can write out records to one or more streams.

```yaml
version: "3.0"

module:
  meta: 
    type: remote
  spec:
    egresses:
      - egress:
          meta: 
            type: io.statefun.kinesis/egress
            id: com.example/out
          spec:
            awsRegion:
              type: specific
              id: eu-west-1
            awsCredentials:
              type: default
            maxOutstandingRecords: 9999
            clientConfigProperties:
              - ThreadingModel: POOLED
              - ThreadPoolSize: 10
```

Please refer to the Kinesis [producer default configuration properties](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties) documentation for the full list of available properties.

## AWS Region

Both the Kinesis ingress and egress can be configured to a specific AWS region.

#### Default Provider Chain (default)

Consults AWS's default provider chain to determine the AWS region.

```yaml
awsCredentials:
  type: default
```

#### Specific

Specifies an AWS region using the region's unique id.

```yaml
awsCredentials:
  type: specific
  id: us-west-1
```

#### Custom Endpoint

Connects to an AWS region through a non-standard AWS service endpoint.
This is typically used only for development and testing purposes.

```yaml
awsCredentials:
  type: custom-endpoint
  endpoint: https://localhost:4567
  id: us-west-1
```

## AWS Credentials

Both the Kinesis ingress and egress can be configured using standard AWS credential providers.

#### Default Provider Chain (default)

Consults AWS’s default provider chain to determine the AWS credentials.

```yaml
awsCredentials:
  type: default
```

#### Basic

Specifies the AWS credentials directly with provided access key ID and secret access key strings.

```yaml
awsCredentials:
  type: basic
  accessKeyId: access-key-id
  secretAccessKey: secret-access-key
```

#### Profile

Specifies the AWS credentials using an AWS configuration profile, along with the profile's configuration path.

```yaml
awsCredentials:
  type: basic
  profileName: profile-name
  profilePath: /path/to/profile/config
```