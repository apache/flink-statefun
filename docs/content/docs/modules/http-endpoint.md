---
title: 'HTTP Function Endpoint'
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

# HTTP Function Endpoint

An HTTP Function Endpoint component defines the endpoint URL that the Stateful Functions runtime should connect to for
invoking a given function, or for a more dynamic deployment, functions within a specified namespace.

Below is an example of an HTTP endpoint definition in an application's module configuration:

{{< tabs "8951ef0a-cdd4-40d1-bda8-dec1299aaf41" >}}
{{< tab "v2 (latest)" >}}
```yaml
kind: io.statefun.endpoints.v2/http
spec:
  functions: com.example/*
  urlPathTemplate: https://bar.foo.com:8080/{function.name}
  transport:
    timeouts:
      call: 1 min
      read: 10 sec
      write: 10 sec
```
{{< /tab >}}
{{< tab "v1" >}}
```yaml
kind: io.statefun.endpoints.v1/http
spec:
  functions: com.example/*
  urlPathTemplate: https://bar.foo.com:8080/{function.name}
  timeouts:
    call: 1 min
    read: 10 sec
    write: 10 sec
```
{{< /tab >}}
{{< /tabs >}}

In this example, an endpoint for a function within the logical namespace `com.example` is declared.
The runtime will invoke all functions under this namespace with the endpoint URL template.

### URL Template

The URL template name may contain template parameters filled in dynamically based on the function's specific type.
In the example below, a message sent to message type `com.example/greeter` will be sent to `http://bar.foo.com/greeter`.

```yaml
spec:
  functions: com.example/*
  urlPathTemplate: https://bar.foo.com/{function.name}
```

Templating parameterization works well with load balancers and service gateways.
Suppose `http://bar.foo.com` was an [NGINX](https://www.nginx.com/) server; you can use the different paths to physical systems. Users may now deploy some functions on Kubernetes, others AWS Lambda, while others are still on physical servers.

{{< img src="/fig/dispatch.png" alt="function dispatch" width="75%" >}}

### Transport

Switching between different transport clients is supported since `v2` of HTTP function endpoint definitions.

The transport client to use is specified using `spec.transport.type`. If not specified, by default, Stateful Functions uses [OkHttp](https://square.github.io/okhttp/).

All fields under `spec.transport` is used as the properties to configure the transport client. For example, the example below configures various timeout settings for the default `OkHttp` transport:

```yaml
spec:
  transport:
    timeouts:
      call: 1 min
      read: 30 sec
      write: 20 sec
```

#### Asynchronous HTTP transport (Beta)

Alternatively, Stateful Functions also ships a transport option based on asynchronous non-blocking IO, implemented with [Netty](https://netty.io/).
This transport enables much higher resource utilization, higher throughput, and lower remote function invocation latency.

Below is a complete example of the `transport` section of an HTTP function endpoint definition, if you want to use this transport type:

```yaml
spec:
  transport:
    type: io.statefun.transports.v1/async
    call: 2m
    connect: 20s
    pool_ttl: 15s
    pool_size: 1024
    payload_max_bytes: 33554432
```

Please see the full spec options below for a description of each property.

## Full Spec Options

#### Target functions (or function)

The meta `typename` is the logical type name used to match a function invocation with a physical endpoint.
Typenames are composed of a namespace (required) and name (optional).
Endpoints containing a namespace but no name will match all function types in that namespace.

{{< hint info >}}
It is recommended to have endpoints only specified against a namespace to enable dynamic function registration.
{{< /hint >}}

```yaml
spec:
  functions: com.example/*
```

#### Url Path Template

The `urlPathTemplate` is the physical path to be resolved when an endpoint is matched.
It may contain templated parameters for dynamic routing.

```yaml
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
spec:
  maxNumBatchRequests: 1000
```

#### Transport

The transport client type to use for sending requests to the function. If not specified, by default, Stateful Functions uses [OkHttp](https://square.github.io/okhttp/).

Below is a full example for configuring the default transport:

```yaml
spec:
  transport:
    timeouts:
      call: 2m
      connect: 20s
      read: 10s
      write: 10s
```

* `call`: The timeout for a complete function call. This configuration spans the entire call: resolving DNS, connecting,
  writing the request body, server processing, and reading the response body. If the call requires redirects or retries
  all must complete within one timeout period. Default value is 1 minute.
* `connect`: The default connect timeout for new connections. The connect timeout is applied when connecting a TCP socket to the target host. Default value is 10 seconds.
* `read`: The default read timeout for new connections. The read timeout is applied to both the TCP socket and for individual read IO operations. Default value is 10 seconds.
* `write`: The default write timeout for new connections. The write timeout is applied for individual write IO operations. Default value is 10 seconds.

Alternatively, a transport client based on asynchronous non-blocking IO is supported:

```yaml
spec:
  transport:
    type: io.statefun.transports.v1/async
    call: 2m
    connect: 20s
    pool_ttl: 15s
    pool_size: 1024
    payload_max_bytes: 33554432
    trust_cacerts: ~/trustedCAs.pem
    client_cert: classpath:clientPublic.crt
    client_key: ~/clientPrivate.key
    client_key_password: /tmp/password.txt
```

* `call`: total duration of a single request (including retries, and backoffs). After this duration, the call is considered failed.
* `connect`: the total amount of time to wait for a successful TCP connection. After that amount of time, an attempt is considered failed, and if the total call time has not elapsed, an additional attempt will be scheduled (after a backoff).
* `pool_ttl`: the amount of time a connection will live in the connection pool. Set to 0 to disable, otherwise the connection will be evicted from the pool after (approximately) that time. If a connection is evicted while it is serving a request, that connection will be only marked for eviction and will be dropped from the pool once the request returns.
* `pool_size`: the maximum pool size.
* `payload_max_bytes`: the maximum size for a request or response payload size. The default is set to 32MB.
* `trust_cacerts`: Trusted public certificate authority certificates in a pem format. If none are provided, but the function uses https, the default jre truststore will be used. If you need to provide more than one CA cert, concat them with a newline in between. This can be taken from a classpath (e.g. classpath:file.pem) or a path.
* `client_cert`: Client public certificate used for mutual tls authentication. This can be taken from a classpath (e.g. classpath:file.crt) or a path
* `client_key`: PKCS8 client private key used for mutual tls authentication. This can be taken from a classpath (e.g. classpath:file.key) or a path
* `client_key_password`: The location of a file containing the client key password (if required). This can be taken from a classpath (e.g. classpath:file.key) or a path


{{< hint info >}}
We highly recommend setting `statefun.async.max-per-task` to a much higher value (see [Configurations]({{< ref "docs/deployment/configurations">}}))
when using the asynchronous transport type. This configuration dictates the maximum amount of in-flight requests sent to
the function before applying back pressure. The default value of this configuration is `1024`, which makes sense only for
the default threaded blocking `OkHttp` transport. For the asynchronous transport, a much higher value is recommended.
{{< /hint >}}