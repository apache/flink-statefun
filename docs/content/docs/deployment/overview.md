---
title: 'Overview'
weight: 1
type: docs
aliases:
  - /deployment-and-operations/
  - /deployment-and-operations/packaging.html
  - /docs/deployment-and-operations/packaging.html
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

# Deployment and Operations

Stateful Functions is a framework built on top of the Apache Flink runtime, which means it inherits Flink's deployment and operations model, and there are no new concepts you need to learn.
Read through the official [Apache Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/) to learn how to run and maintain an application in production.
The following pages outline Stateful Functions' specific concepts, configurations, and metrics.

## Images 

The recommended deployment mode for Stateful Functions applications is to build a Docker image. This way, user code does not need to package any Apache Flink components. The provided base image allows teams to package their applications with all the necessary runtime dependencies quickly.

The community provides images containing the entire Stateful Functions runtime: `flink-statefun:{{< version >}}`. 
All you need to do is add your [module configurations]({{< ref "docs/deployment/module" >}}) to the image, or attached as a config map to the container. 

{{< stable >}}
{{< hint info >}}
The Flink community is currently waiting for the official Docker images to be published to Docker Hub.
In the meantime, Ververica has volunteered to make Stateful Functions' images available via their public registry: 

```docker
FROM ververica/flink-statefun:{{< version >}}
```

You can follow the status of Docker Hub contribution [here](https://github.com/docker-library/official-images/pull/7749).
{{< /hint >}}
{{< /stable >}}
{{< unstable >}}
{{< hint info >}}
The Flink community does not publish images for snapshot versions.
You can build this version locally by cloning the [repo](https://github.com/apache/flink-statefun) and following the instructions in `tools/docker/README.md`
{{< /hint >}}
{{< /unstable >}}

## Deployment

The packaged image can then be deployed as a [standalone Flink cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/resource-providers/standalone/kubernetes.html#common-cluster-resource-definitions) on Kubernetes. 

After creating the below [cluster components](#cluster-components), the application can be launched using the `kubectl` command.

```bash
# Configuration and service definition
$ kubectl create -f application-module.yaml
$ kubectl create -f flink-config.yaml
$ kubectl create -f jobmanager-service.yaml

# Deploy the StateFun runtime
$ kubectl create -f jobmanager-job.yaml
$ kubectl create -f taskmanager-job-deployment-yaml
```

To terminate the cluster, simply delete the deployments. 

```bash
$ kubectl delete -f taskmanager-job-deployment-yaml
$ kubectl delete -f jobmanager-job.yaml
$ kubectl delete -f jobmanager-service.yaml
$ kubectl delete -f flink-config.yaml
$ kubectl delete -f application-module.yaml
```

### Cluster Components

{{< details "application-module.yaml" >}}
The application function and io module configuration.
See the full [documentation]({{< ref "docs/deployment/module" >}}) for more details.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: application-module
  namespace: statefun
  labels:
    app: statefun
data:
  module.yaml: |+
    version: "3.0"
    module:
      meta:
        type: remote
      spec:
        endpoints:
        ingresses:
        egresses:
```
{{< /details >}}

{{< details "flink-config.yaml" >}}
`flink-config.yaml` contains all Flink runtime configurations for the cluster, 
including both [Apache Flink configurations](https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/config.html) and [StatFun specific configurations]({{< ref "docs/deployment/configurations" >}}).
In the Apache Flink documentation, you will often see this referred to as the `flink-conf.yaml` file.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  labels:
    app: statefun
data:
  flink-conf.yaml: |+
    jobmanager.rpc.address: statefun-master
    taskmanager.numberOfTaskSlots: 1
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    classloader.parent-first-patterns.additional: org.apache.flink.statefun;org.apache.kafka;com.google.protobuf
    state.checkpoints.dir: s3://my-checkpoint-bucket
    state.backend: rocksdb
    state.backend.rocksdb.timer-service.factory: ROCKSDB
    state.backend.incremental: true
    execution.checkpointing.interval: 10sec
    execution.checkpointing.mode: EXACTLY_ONCE
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: 2147483647
    restart-strategy.fixed-delay.delay: 1sec
    jobmanager.memory.process.size: 1g
    taskmanager.memory.process.size: 1g
    parallelism.default: 3
  log4j-console.properties: |+
    rootLogger.level = INFO
    rootLogger.appenderRef.console.ref = ConsoleAppender
    rootLogger.appenderRef.rolling.ref = RollingFileAppender
    logger.akka.name = akka
    logger.akka.level = INFO
    logger.kafka.name= org.apache.kafka
    logger.kafka.level = INFO
    logger.hadoop.name = org.apache.hadoop
    logger.hadoop.level = INFO
    logger.zookeeper.name = org.apache.zookeeper
    logger.zookeeper.level = INFO
    appender.console.name = ConsoleAppender
    appender.console.type = CONSOLE
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
    appender.rolling.name = RollingFileAppender
    appender.rolling.type = RollingFile
    appender.rolling.append = false
    appender.rolling.fileName = ${sys:log.file}
    appender.rolling.filePattern = ${sys:log.file}.%i
    appender.rolling.layout.type = PatternLayout
    appender.rolling.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
    appender.rolling.policies.type = Policies
    appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
    appender.rolling.policies.size.size=100MB
    appender.rolling.strategy.type = DefaultRolloverStrategy
    appender.rolling.strategy.max = 10
    logger.netty.name = org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline
    logger.netty.level = OFF
```
{{< /details >}}

{{< details "jobmanager-service.yaml" >}}
```yaml
`jobmanager-service.yaml`
```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
spec:
  type: ClusterIP
  ports:
  - name: rpc
    port: 6123
  - name: blob-server
    port: 6124
  - name: webui
    port: 8081
  selector:
    app: statefun
    component: jobmanager
```
{{< /details >}}

{{< details "jobmanager-rest-service.yaml" >}}
An optional service that exposes the jobmanager `rest` port as a public Kubernetes nodes port.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: statefun-jobmanager-rest
spec:
  type: NodePort
  ports:
  - name: rest
    port: 8081
    targetPort: 8081
    nodePort: 30081
  selector:
    app: statefun
    component: jobmanager
```
{{< /details >}}

{{< details "jobmanager-application.yaml" >}}
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: statefun-jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: statefun
      component: jobmanager
  template:
    metadata:
      labels:
        app: statefun
        component: master
    spec:
      containers:
        - name: master
          image: flink-statefun:{{< version >}}
          imagePullPolicy: Always
          env:
            - name: ROLE
              value: master
            - name: MASTER_HOST
              value: statefun-jobmanager
          resources:
            requests:
              memory: "1.5Gi"
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob
            - containerPort: 8081
              name: ui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: application-module
              mountPath: /opt/statefun/modules/application-module
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: application-module
          configMap:
            name: application-module
            items:
              - key: module.yaml
                path: module.yaml
```
{{< /details >}}

{{< details "taskmanager-job-deployment.yaml" >}}
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: statefun-worker
  namespace: statefun
spec:
  replicas: 3
  selector:
    matchLabels:
      app: statefun
      component: worker
  template:
    metadata:
      labels:
        app: statefun
        component: worker
    spec:
      containers:
        - name: worker
          image: flink-statefun:{{< version >}}
          imagePullPolicy: Always
          env:
            - name: ROLE
              value: worker
            - name: MASTER_HOST
              value: statefun-jobmanager
          resources:
            requests:
              memory: "1.5Gi"
          ports:
            - containerPort: 6122
              name: rpc
            - containerPort: 6124
              name: blob
            - containerPort: 8081
              name: ui
          livenessProbe:
            tcpSocket:
              port: 6122
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: greeter-module
              mountPath: /opt/statefun/modules/greeter
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: application-module
          configMap:
            name: application-module
            items:
              - key: module.yaml
                path: module.yaml
```
{{< /details >}}