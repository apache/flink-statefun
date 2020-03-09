# Kubernetes example

This example demonstrates how to deploy a stateful function application
written in Python to Kubernetes.

## Prerequisites 

* Helm
* Kubernetes cluster
* Kafka   
* [StateFun distribution](https://github.com/apache/flink-statefun#build)
* [StateFun Python SDK](https://github.com/apache/flink-statefun/blob/master/statefun-python-sdk/build-distribution.sh)


## Overview

This examples create a stateful function application,
that consumes `LoginEvent`s from a `logins` Kafka topic,
and produces `seen` count per user, into the `seen` Kafka topic.

The main example components contains:
- [main.py](main.py) - A StateFun python function that implements the main logic
- [module.yaml](module.yaml) - defines the ingress, egress and the remote function specification.
- [resources](resources) - a Helm chart, templates to deploy StateFun cluster and the remote python worker to k8s. 
- [build-example.sh](build-example.sh) - Builds StateFun Docker images and k8s resources to deploy it. 

## Setup

### Create Kafka Topics: 

This example consumes `LoginEvent`s from the `logins` topic, and produces `SeenCount` to
the `seen` topic
```
 ./kafka-topics.sh --create --topic logins --zookeeper <zookeeper address>:2181 --partitions 1 --replication-factor 1
 ./kafka-topics.sh --create --topic seen --zookeeper <zookeeper address>:2181 --partitions 1 --replication-factor 1
```

### update [module.yaml](module.yaml)

Make sure that your `module.yaml` ingress/and egress sections point to your
Kafka cluster. 

```
ingresses:
      - ingress:
            ...
          spec:
            address: kafka-service:9092
            ...
    egresses:
      - egress:
            ...
          spec:
            address: kafka-service:9092
```

### Build the Docker images and the k8s resource yamls.

This examples creates two different Docker images, one for the `Python` remote 
worker (`k8s-demo-python-worker`) and one for the statefun cluster (`k8s-demo-statefun`).

- If you have a remote docker registry (i.e. `gcr.io/<project-name>`) make sure
to update [resources/values.yaml](resources/values.yaml) relevant `image: ` sections.

- Modify [resources/values.yaml](resources/values.yaml) and set the value of `checkpoint.directory`
to a filesystem / object store. For example
```
checkpoint:
  dir: gcs://my-project/my-bucket
```  


Assuming the all prerequisites where completed run:

```build-example.sh```

This should create the Docker images and generate a `k8s-demo.yaml` file.

## Deploy

`kubectl create -f k8s-demo.yaml`
 
## Generate events

Run:

```
pip3 install kafka-python 
python3 event_generator.py --address <kafka address> --events 1000
```

This would generate 1,000 login events into the `logins` topic