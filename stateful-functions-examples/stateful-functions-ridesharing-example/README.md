# The Ridesharing Example

The Ridesharing example is a more complicated Stateful Functions example consisting of 4 different functions, each
corresponding to a real-world entity in a ridesharing scenario: `FnDriver`, `FnGeoCell`, `FnPassenger`, and `FnRide`.

The whole example also includes a simulator program, which simulates real-world drivers and passengers sending
events to the Stateful Functions application. Driver simulations will be sending their location updates to the
application, while passenger simulations will be sending ride requests.

## Running the example

To run the example:

```
docker-compose build
docker-compose up
```

This starts both the simulator program and Stateful Functions ridesharing example application.

After all the components have fully started, you can take a look at the web UI of the Flink Jobmanager to see the
application running, at `localhost:8081`.

Then, you need to issue a request to the simulator program to start the simulation:

```
curl -X POST localhost:5656/api/start
```
