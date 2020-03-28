---
title: "Walkthrough"
nav-id: walkthrough
nav-title: 'Walkthrough'
nav-parent_id: getting-started
nav-pos: 1
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

Stateful Functions offers a platform for building robust, stateful event-driven applications.
It provides fine-grained control over state and time, which allows for the implementation of advanced systems.
In this step-by-step guide you’ll learn how to build a stateful applications with the Stateful Functions API.

* This will be replaced by the TOC
{:toc}

## What Are You Building?

Like all great introductions in software, this walkthrough will start at the beginning: saying hello.
The application will run a simple function that accepts a request and responds with a greeting.
It will not attempt to cover all the complexities of application development, but instead focus on building a stateful function — which is where you will implement your business logic.

## Prerequisites

This walkthrough assumes that you have some familiarity with Python, but you should be able to follow along even if you are coming from a different programming language.

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/gettinghelp.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) is consistently ranked as one of the most active of any Apache project and a great way to get help quickly.

## How to Follow Along

If you want to follow along, you will require a computer with [Python 3](https://www.python.org/) along with [Docker](https://www.docker.com/).

{% panel **Note:** Each code block within this walkthrough may not contain the full surrounding class for brevity.
The full code is available on [at the bottom of this page](#full-application). %}

You can download a zip file with a skeleton project by clicking [here]({{ site.baseurl }}/downloads/walkthrough.zip).

{% unless site.is_stable %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>Note</b>: The Stateful Functions project does not publish snapshot versions of the Python SDK to PyPy.
    Please consider using a stable version of this guide.
</p>
{% endunless %}

After unzipping the package, you will find a number of files.
These include dockerfiles and data generators to run this walkthrough in a local self contained environment.

{% highlight bash %}
$ tree statefun-walkthrough
statefun-walkthrough
├── Dockerfile
├── README.md
├── docker-compose.yml
├── generator
│   ├── Dockerfile
│   ├── event-generator.py
│   └── messages_pb2.py
├── greeter
│   ├── Dockerfile
│   ├── greeter.py
│   ├── messages.proto
│   ├── messages_pb2.py
│   └── requirements.txt
└── module.yaml
{% endhighlight %}

## Start With Events

Stateful Functions is an event driven system, so development begins by defining our events.
The greeter application will define its events using [protocol buffers](https://developers.google.com/protocol-buffers). 
When a greet request for a particular user is ingested, it will be routed to the appropriate function.
The response will be returned with an appropriate greeting.
The third type, `SeenCount`, is a utility class that will be used latter on to help manage the number of times a user has been seen so far.

{% highlight proto %}
syntax = "proto3";

package example;

// External request sent by a user who wants to be greeted
message GreetRequest {
    // The name of the user to greet
    string name = 1;
}
// A customized response sent to the user
message GreetResponse {
    // The name of the user being greeted
    string name = 1;
    // The users customized greeting
    string greeting = 2;
}
// An internal message used to store state
message SeenCount {
    // The number of times a users has been seen so far
    int64 seen = 1;
}
{% endhighlight %}


## Our First Function

Under the hood, messages are processed using [stateful functions]({{ site.baseurl }}/sdk/python.html), which is any two argument function that is bound to the ``StatefulFunction`` runtime.
Functions are bound to the runtime with the `@function.bind` decorator.
When binding a function, it is annotated with a function type.
This is the name used to reference this function when sending it messages.

When you open the file `greeter/greeter.py` you should see the following code.

{% highlight python %}
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/greeter")
def greet(context, greet_request):
    pass
{% endhighlight %}

A stateful function takes two arguments, a context and message. 
The [context]({{ site.baseurl }}/sdk/python.html#context-reference) provides access to stateful functions runtime features such as state management and message passing.
You will explore some of these features as you progress through this walkthrough. 

The other parameter is the input message that has been passed to this function.
By default messages are passed around as protobuf [Any](https://developers.google.com/protocol-buffers/docs/reference/python-generated#wkt).
If a function only accepts a known type, you can override the message type using Python 3 type syntax.
This way you do not need to unwrap the message or check types.

{% highlight python %}
from messages_pb2 import GreetRequest
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/greeter")
def greet(context, greet_request: GreetRequest):
    pass
{% endhighlight %}

## Sending A Response

Stateful Functions accept messages and can also send them out.
Messages can be sent to other functions, as well as external systems (or [egress]({{ site.baseurl }}/io-module/index.html#egress)).

One popular external system is [Apache Kafka](http://kafka.apache.org/).
As a first step, lets update our function in `greeter/greeter.py` to respond to each input by sending a greeting to a Kafka topic.

{% highlight python %}
from messages_pb2 import GreetRequest, GreetResponse
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/greeter")
def greet(context, message: GreetRequest):
    response = GreetResponse()
    response.name = message.name
    response.greeting = "Hello {}".format(message.name)
    
    egress_message = kafka_egress_record(topic="greetings", key=message.name, value=response)
    context.pack_and_send_egress("example/greets", egress_message)
{% endhighlight %} 

For each message, a response is constructed and sent to a kafka topic call `greetings` partitioned by `name`.
The `egress_message` is sent to a an `egress` named `example/greets`.
This identifier points to a particular Kafka cluster and is configured on deployment below.

## A Stateful Hello

This is a great start, but does not show off the real power of stateful functions - working with state.
Suppose you want to generate a personalized response for each user depending on how many times they have sent a request.

{% highlight python %}
def compute_greeting(name, seen):
    """
    Compute a personalized greeting, based on the number of times this @name had been seen before.
    """
    templates = ["", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"]
    if seen < len(templates):
        greeting = templates[seen] % name
    else:
        greeting = "Nice to see you at the %d-nth time %s!" % (seen, name)

    response = GreetResponse()
    response.name = name
    response.greeting = greeting

    return response
{% endhighlight %}

To “remember” information across multiple greeting messages, you then need to associate a persisted value field (``seen_count``) to the Greet function.
For each user, functions can now track how many times they have been seen.

{% highlight python %}
@functions.bind("example/greeter")
def greet(context, greet_message: GreetRequest):
    state = context.state('seen_count').unpack(SeenCount)
    if not state:
        state = SeenCount()
        state.seen = 1
    else:
        state.seen += 1
    context.state('seen_count').pack(state)

    response = compute_greeting(greet_request.name, state.seen)

    egress_message = kafka_egress_record(topic="greetings", key=greet_request.name, value=response)
    context.pack_and_send_egress("example/greets", egress_message)
{% endhighlight %}

The state, `seen_count` is always scoped to the current name so it can track each user independently.

## Wiring It All Together

Stateful Function applications communicate with the Apache Flink runtime using `http`.
The Python SDK ships with a ``RequestReplyHandler`` that automatically dispatches function calls based on RESTful HTTP ``POSTS``.
The ``RequestReplyHandler`` may be exposed using any HTTP framework.

One popular Python web framework is [Flask](https://palletsprojects.com/p/flask/).
It can be used to quickly and easily expose an application to the Apache Flink runtime.

{% highlight python %}
from statefun import StatefulFunctions
from statefun import RequestReplyHandler

functions = StatefulFunctions()

@functions.bind("walkthrough/greeter")
def greeter(context, message: GreetRequest):
    pass

handler = RequestReplyHandler(functions)

# Serve the endpoint

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)

@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run()
{% endhighlight %}

## Configuring for Runtime

The Stateful Function runtime makes requests to the greeter function by making `http` calls to the `Flask` server.
To do that, it needs to know what endpoint it can use to reach the server.
This is also a good time to configure our connection to the input and output Kafka topics.
The configuration is in a file called `module.yaml`.

{% highlight yaml %}
version: "1.0"
module:
  meta:
    type: remote
  spec:
    functions:
      - function:
          meta:
            kind: http
            type: example/greeter
          spec:
            endpoint: http://python-worker:8000/statefun
            states:
              - seen_count
            maxNumBatchRequests: 500
            timeout: 2min
    ingresses:
      - ingress:
          meta:
            type: statefun.kafka.io/routable-protobuf-ingress
            id: example/names
          spec:
            address: kafka-broker:9092
            consumerGroupId: my-group-id
            topics:
              - topic: names
                typeUrl: com.googleapis/example.GreetRequest
                targets:
                  - example/greeter
    egresses:
      - egress:
          meta:
            type: statefun.kafka.io/generic-egress
            id: example/greets
          spec:
            address: kafka-broker:9092
            deliverySemantic:
              type: exactly-once
              transactionTimeoutMillis: 100000
{% endhighlight %}

This configuration does a few interesting things.

The first is to declare our function, `example/greeter`.
It includes the endpoint by which it is reachable along with the states the function has access to.

The ingress is the input Kafka topic that routes `GreetRequest` messages to the function.
Along with basic properties like broker address and consumer group, it contains a list of targets.
These are the functions each message will be sent to.

The egress is the output Kafka cluster.
It contains broker specific configurations but allows each message to route to any topic.

## Deployment

Now that the greeter application has been built it is time to deploy. 
The simplest way to deploy a Stateful Function application is by using the community provided base image and loading your module.
The base image provides the Stateful Function runtime, it will use the provided `module.yaml` to configure for this specific job.
This can be found in the `Dockerfile` in the root directory. 

{% highlight docker %}
FROM flink-statefun:{{ site.version }}

RUN mkdir -p /opt/statefun/modules/greeter
ADD module.yaml /opt/statefun/modules/greeter
{% endhighlight %}

You can now run this application locally using the provided Docker setup.

{% highlight bash %}
$ docker-compose up -d
{% endhighlight %}

Then, to see the example in actions, see what comes out of the topic `greetings`:

{% highlight bash %}
docker-compose logs -f event-generator 
{% endhighlight %}


## Want To Go Further?

This Greeter never forgets a user.
Try and modify the function so that it will reset the ``seen_count`` for any user that spends more than 60 seconds without interacting with the system.

## Full Application 

{% highlight python %}
from messages_pb2 import SeenCount, GreetRequest, GreetResponse

from statefun import StatefulFunctions
from statefun import RequestReplyHandler
from statefun import kafka_egress_record

functions = StatefulFunctions()

@functions.bind("example/greeter")
def greet(context, greet_request: GreetRequest):
    state = context.state('seen_count').unpack(SeenCount)
    if not state:
        state = SeenCount()
        state.seen = 1
    else:
        state.seen += 1
    context.state('seen_count').pack(state)

    response = compute_greeting(greet_request.name, state.seen)

    egress_message = kafka_egress_record(topic="greetings", key=greet_request.name, value=response)
    context.pack_and_send_egress("example/greets", egress_message)


def compute_greeting(name, seen):
    """
    Compute a personalized greeting, based on the number of times this @name had been seen before.
    """
    templates = ["", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"]
    if seen < len(templates):
        greeting = templates[seen] % name
    else:
        greeting = "Nice to see you at the %d-nth time %s!" % (seen, name)

    response = GreetResponse()
    response.name = name
    response.greeting = greeting

    return response


handler = RequestReplyHandler(functions)

#
# Serve the endpoint
#

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)


@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run()

{% endhighlight %}
