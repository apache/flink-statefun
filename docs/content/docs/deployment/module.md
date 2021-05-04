---
title: Module Configuration
weight: 2
type: docs
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

# Module Configuration

An application's module configuration contains all necessary runtime information to configure the Stateful Functions runtime for a specific application. It includes the endpoints where functions can be reached along with ingress and egress definitions.

```yaml
version: "3.0"

module:
  meta: 
    type: remote
  spec:
    endpoints:
      meta:
        kind: http
      spec:
        functions: com.example/*
        urlPathTempalte: https://bar.foo.com/{function.name}
    ingresses:
      - ingress:
        meta:
          type: io.statefun.kafka/ingress
          id: com.example/my-ingress
        spec:
          address: kafka-broker:9092
          consumerGroupId: my-consumer-group
          startupPosition:
            type: earliest
          topics:
            - topic: message-topic
              valueType: io.statefun.types/string
              targets:
                - com.example/greeter
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
```

## Endpoint Definition

`module.spec.endpoints` declares a list of `endpoint` objects containing all metadata the Stateful Function runtime needs to invoke a function. The following is an example of an endpoint definition for a single function. 

```yaml
endpoints:
  - endpoint:
    meta: 
      kind: http
    spec:
      functions: com.example/*
      urlPathTemplate: https://bar.foo.com/{function.name}
```

In this example, an endpoint for a function within the logical namespace `com.example` is declared.
The runtime will invoke all functions under this namespace with the endpoint URL template. 

## URL Template

The URL template name may contain template parameters that are filled in based on the function's specific type.
For example, a message sent to message type `com.example/greeter` will be sent to `http://bar.foo.com/greeter`. 

```yaml
endpoints:
  - endpoint:
    meta: 
      kind: http
      functions: com.example/* 
    spec:
      urlPathTemplate: https://bar.foo.com/{function.name}
```

Templating parameterization works well with load balancers and service gateways. 
Suppose `http://bar.foo.com` was an [NGINX](https://www.nginx.com/) server, you can use the different paths to physical systems. Users may now deploy some functions on Kubernetes, others AWS Lambda, while others still on physical servers.

{{< img src="/fig/dispatch.png" alt="function dispatch" width="75%" >}}

#### Example Using NGINX

```
http {
    server {
        listen 80;
        server_name proxy;

        location /greeter {
            proxy_pass http://greeter:8000/service;
        }

        location /emailsender {
            proxy_pass http://emailsender:8000/service;
        }
    }
}

events {}
```

This pattern makes the function's physical deployment transparent to the Stateful Functions runtime.
They can be deployed, upgraded, rolled back and scaled transparently from the cluster. 

### Full Options

#### Protocol

The RPC protocol used to communicate by the runtime to communicate with the function.
``http`` is currently the only supported value.

```yaml
endpoint:
  meta:
    kind: http
  spec:
```

#### Typename

The meta `typename` is the logical type name used to match a function invocation with a physical endpoint.
Typenames are composed of a namespace (required) and name (optional).
Endpoints containing a namespace but no name will match all function types in that namespace.

{{< hint info >}}
It is recommended to have endpoints only specified against a namespace to enable dynamic function registration.
{{< /hint >}}

```yaml
endpoint:
  meta:
    kind: http
    functions: com.example/*
  spec:
```

#### Url Path Template

The `urlPathTemplate` is the physical path to be resolved when an endpoint is matched.
It may contain templated parameters for dynamic routing.

```yaml
endpoint:
  meta:
  spec:
    urlPathTemplate: http://bar.foo.com/{function.name}
```

Supported schemes: 
* ``http``
* ``https``

Transport via UNIX domain sockets is supported by using the schemes ``http+unix`` or ``https+unix``.
When using UNIX domain sockets, the endpoint format is: ``http+unix://<socket-file-path>/<serve-url-path>``. For example, ``http+unix:///uds.sock/path/of/url``.
For example, ``http+unix:///uds.sock/path/of/url``.

#### Max Batch Requests

The maximum number of records that can be processed by a function for a particular address (typename + id) before invoking backpressure on the system. The default value is `1000`. 

```yaml
endpoint:
  meta:
  spec:
    maxNumBatchRequests: 1000
```

#### Timeouts

Timeouts is an optional object containing various timeouts for a request before a function is considered unreachable.

**Call**

The timeout for a complete function call.
This configuration spans the entire call: resolving DNS, connecting, writing the request body, server processing, and reading the response body.
If the call requires redirects or retries all must complete within one timeout period.

```yaml
endpoint: 
  meta:
  spec:
    timeout:
      call: 1 min
```

**Connect**

The default connect timeout for new connections.
The connect timeout is applied when connecting a TCP socket to the target host.

```yaml
endpoint: 
  meta:
  spec:
    timeout:
      connect: 10 sec
```

**Read**

The default read timeout for new connections.
The read timeout is applied to both the TCP socket and for individual read IO operations.

```yaml
endpoint: 
  meta:
  spec:
    timeout:
      read: 10 sec
```

**Write**

The default write timeout for new connections.
The write timeout is applied for individual write IO operations.

```yaml
endpoint: 
  meta:
  spec:
    timeout:
      write: 10 sec
```

## Ingress

An ingress is an input point where data is consumed from an external system and forwarded to zero or more functions.
It is defined via an identifier and specification.

An ingress identifier, similar to a function type, uniquely identifies an ingress by specifying its [input type]({{< ref "docs/sdk/appendix#types" >}}), a namespace, and a name.

The spec defines the details of how to connect to the external system, which is specific to each individual I/O module. Each identifier-spec pair is bound to the system inside an stateful function module.

```yaml
version: "3.0"

module:
  meta:
    type: remote
  spec:
    ingresses:
      - ingress:
          meta:
            id: example/my-ingress
            type: # ingress type
          spec: # ingress specific configurations
```

## Egress

```yaml
version: "3.0"

module:
  meta:
    type: remote
  spec:
    egresses:
      - egress:
        meta:
          id: example/my-egress
          type: # egress type
        spec: # egress specific configurations
```
