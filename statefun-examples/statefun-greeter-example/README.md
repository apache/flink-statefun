# The Greeter Example

This is a simple example that runs a simple stateful function that accepts requests from a Kafka ingress,
and then responds by sending greeting responses to a Kafka egress. It demonstrates the primitive building blocks
of a Stateful Functions applications, such as ingresses, routing messages to functions, handling state in functions,
and sending messages to egresses.

## Running the example

To run the example:

```
docker-compose build
docker-compose up
```

Then, to see the example in actions, send some messages to the topic `names`, and see what comes out
out of the topic `greetings`:

```
docker-compose exec kafka-broker kafka-console-producer.sh \
     --broker-list localhost:9092 \
     --topic names
```

```
docker-compose exec kafka-broker kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic greetings --from-beginning
```
