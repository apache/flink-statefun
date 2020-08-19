# The Greeter Example

This is a simple example that runs a asynchrouns stateful function that accepts requests from a Kafka ingress,
and then responds by sending greeting responses to a Kafka egress. It demonstrates the primitive building blocks
of a Stateful Functions applications, such as ingresses, handling state in functions,
and sending messages to egresses.


## Building the example

1) Make sure that you have built the Python distribution
   To build the distribution
    -  `cd statefun-python-sdk/`
    -  `./build-distribution.sh`
    
2) Run `./build-example.sh` 

## Running the example

To run the example:

```
./build-example.sh
docker-compose up -d
```

Then, to see the example in actions, see what comes out of the topic `greetings`:

```
docker-compose logs -f event-generator
```

