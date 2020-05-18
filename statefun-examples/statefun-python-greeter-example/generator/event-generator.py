################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import signal
import sys
import time
import threading

import random

from kafka.errors import NoBrokersAvailable

from messages_pb2 import GreetRequest, GreetResponse

from kafka import KafkaProducer
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
NAMES = ["Jerry", "George", "Elaine", "Kramer", "Newman", "Frank"]


def random_requests():
    """Generate infinite sequence of random GreetRequests."""
    while True:
        request = GreetRequest()
        request.name = random.choice(NAMES)
        yield request


def produce():
    if len(sys.argv) == 2:
        delay_seconds = int(sys.argv[1])
    else:
        delay_seconds = 1
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    for request in random_requests():
        key = request.name.encode('utf-8')
        val = request.SerializeToString()
        producer.send(topic='names', key=key, value=val)
        producer.flush()
        time.sleep(delay_seconds)


def consume():
    consumer = KafkaConsumer(
        'greetings',
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        group_id='event-gen')
    print("Starting the consumer")
    for message in consumer:
        print("New message!")
        response = GreetResponse()
        response.ParseFromString(message.value)
        print("%s:\t%s" % (response.name, response.greeting), flush=True)


def handler(number, frame):
    sys.exit(0)


def safe_loop(fn):
    while True:
        try:
            fn()
        except SystemExit:
            print("Good bye!")
            return
        except NoBrokersAvailable:
            print("Cannot find a damn broker")
            time.sleep(2)
            continue
        except Exception as e:
            print(e)
            return


def main():
    signal.signal(signal.SIGTERM, handler)

    producer = threading.Thread(target=safe_loop, args=[produce])
    producer.start()

    consumer = threading.Thread(target=safe_loop, args=[consume])
    consumer.start()

    producer.join()
    consumer.join()


if __name__ == "__main__":
    print("Starting!")
    main()
