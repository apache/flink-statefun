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

import sys
import getopt
import random
import string

from messages_pb2 import LoginEvent
from messages_pb2 import SeenCount

from kafka import KafkaProducer


def random_user(n):
    """generate a random user id of size n"""
    chars = []
    for i in range(n):
        chars.append(random.choice(string.ascii_lowercase))
    return ''.join(chars)


def produce(events, address):
    producer = KafkaProducer(bootstrap_servers=address)

    for _ in range(events):
        event = LoginEvent()
        event.user_name = random_user(4)
        producer.send('logins', event.SerializeToString())
    producer.flush()
    producer.close()


def usage():
    print('usage: python3 event-generator.py --address=localhost:9092 --events=1')
    sys.exit(1)


def parse_args():
    address = None
    events = None
    opts, args = getopt.getopt(sys.argv[1:], "a:e", ["address=", "events="])
    for opt, arg in opts:
        if opt in ("-a", "--address"):
            address = arg
        elif opt in ("-e", "--events"):
            events = arg
    if address is None or events is None:
        usage()
    return address, int(events)


def main():
    address, events = parse_args()
    produce(address=address, events=events)


if __name__ == "__main__":
    main()
