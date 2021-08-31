---
title: 'Overview'
weight: 1
type: docs
aliases:
  - /modules/
permalink: /modules/index.html
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

# Application Modules

An application module consists of multiple [components]({{< ref "docs/concepts/application-building-blocks" >}})
that take part in a StateFun application. It includes the endpoints where the runtime can reach functions, along with ingress and egress definitions.

Modules are defined using a YAML file. For example, below is a module that defines an HTTP function endpoint as well as
a Kafka ingress and egress:

```yaml
kind: io.statefun.endpoints.v2/http
spec:
  functions: com.example/*
  urlPathTemplate: https://bar.foo.com/{function.name}
---
kind: io.statefun.kafka.v1/ingress
spec:
  id: com.example/my-ingress
  address: kafka-broker:9092
  consumerGroupId: my-consumer-group
  topics:
    - topic: message-topic
      valueType: io.statefun.types/string
      targets:
        - com.example/greeter
---
kind: io.statefun.kafka.v1/egress
spec:
  id: com.example/my-egress
  address: kafka-broker:9092
  deliverySemantic:
    type: exactly-once
    transactionTimeout: 15min
```

A module YAML file can contain multiple YAML documents, separated by `---`, each representing a component to be included in the application.
Each component is defined by a kind typename string and a spec object containing the component's properties.
