import sys

import unittest
from datetime import timedelta

import requests
from google.protobuf.json_format import MessageToDict
from google.protobuf.any_pb2 import Any

from statefun.request_reply_pb2 import ToFunction, FromFunction

from walkthrough_pb2 import Hello, AnotherHello, Counter


class InvocationBuilder(object):
    """builder for the ToFunction message"""

    def __init__(self):
        self.to_function = ToFunction()

    def with_target(self, ns, type, id):
        InvocationBuilder.set_address(ns, type, id, self.to_function.invocation.target)
        return self

    def with_state(self, name, value=None):
        state = self.to_function.invocation.state.add()
        state.state_name = name
        if value:
            any = Any()
            any.Pack(value)
            state.state_value = any.SerializeToString()
        return self

    def with_invocation(self, arg, caller=None):
        invocation = self.to_function.invocation.invocations.add()
        if caller:
            (ns, type, id) = caller
            InvocationBuilder.set_address(ns, type, id, invocation.caller)
        invocation.argument.Pack(arg)
        return self

    def SerializeToString(self):
        return self.to_function.SerializeToString()

    @staticmethod
    def set_address(namespace, type, id, address):
        address.namespace = namespace
        address.type = type
        address.id = id


def post(data):
    return requests.post(url='http://localhost:5000/statefun',
                         data=data,
                         headers={'Content-Type': 'application/octet-stream'})


# --------------------------------------------------------------------------------------------------------------
# example
# ---------------------------------------------------------------------------------------------------------------

class Examples(object):
    def __init__(self):
        self.examples = {}

    def bind(self, typename):
        def wrapper(fn):
            self.examples[typename] = fn
            return fn

        return wrapper

    def invoke(self, typename):
        fn = self.examples[typename]
        builder = InvocationBuilder()
        type, name = typename.split("/")
        builder.with_target(type, name, "some id")
        fn(builder)
        result = post(builder.SerializeToString())
        from_fn = FromFunction()
        from_fn.ParseFromString(result.content)
        print(MessageToDict(from_fn))


examples = Examples()


@examples.bind("walkthrough/hello")
def hello(builder):
    msg = Hello()
    msg.world = "Hello world!"
    builder.with_invocation(msg)


@examples.bind("walkthrough/any")
def any_example(builder):
    hello(builder)


@examples.bind("walkthrough/type-hint")
def typehint(builder):
    hello(builder)


@examples.bind("walkthrough/union-type-hint")
def union_type_hint(builder):
    hello = Hello()
    builder.with_invocation(hello)

    another_hello = AnotherHello()
    builder.with_invocation(another_hello)


@examples.bind("walkthrough/state_access")
def state1(builder):
    builder.with_state("counter")
    builder.with_invocation(Hello())


@examples.bind("walkthrough/state_access_unpack")
def state2(builder):
    counter = Counter()
    counter.value = 1
    builder.with_state("counter", counter)
    builder.with_invocation(Hello())


@examples.bind("walkthrough/state_access_del")
def state3(builder):
    counter = Counter()
    counter.value = 1
    builder.with_state("counter", counter)
    builder.with_invocation(Hello())


@examples.bind("walkthrough/send")
def send(builder):
    hello(builder)


@examples.bind("walkthrough/reply")
def reply(builder):
    reply_to = ("example-runner", "reply", "0")
    builder.with_invocation(Hello(), reply_to)


@examples.bind("walkthrough/egress")
def egress(builder):
    hello(builder)


def main():
    if len(sys.argv) != 2:
        print("usage: run-example.py <ns/name>")
        sys.exit(1)
    example = sys.argv[1]
    examples.invoke(example)


if __name__ == "__main__":
    main()
