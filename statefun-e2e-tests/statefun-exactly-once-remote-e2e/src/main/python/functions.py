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

import uuid
from statefun import *

from remote_module_verification_pb2 import Invoke, InvokeResult

InvokeType = make_protobuf_type(namespace="statefun.e2e", cls=Invoke)
InvokeResultType = make_protobuf_type(namespace="statefun.e2e", cls=InvokeResult)

functions = StatefulFunctions()


@functions.bind(
    typename="org.apache.flink.statefun.e2e.remote/counter",
    specs=[ValueSpec(name='invoke_count', type=IntType)])
def counter(context, message):
    """
    Keeps count of the number of invocations, and forwards that count
    to be sent to the Kafka egress. We do the extra forwarding instead
    of directly sending to Kafka, so that we cover inter-function
    messaging in our E2E test.
    """
    n = context.storage.invoke_count or 0
    n += 1
    context.storage.invoke_count = n

    response = InvokeResult()
    response.id = context.address.id
    response.invoke_count = n

    context.send(
        message_builder(target_typename="org.apache.flink.statefun.e2e.remote/forward-function",
                        # use random keys to simulate both local handovers and
                        # cross-partition messaging via the feedback loop
                        target_id=uuid.uuid4().hex,
                        value=response,
                        value_type=InvokeResultType))


@functions.bind("org.apache.flink.statefun.e2e.remote/forward-function")
def forward_to_egress(context, message):
    """
    Simply forwards the results to the Kafka egress.
    """
    invoke_result = message.as_type(InvokeResultType)

    egress_message = kafka_egress_message(
        typename="org.apache.flink.statefun.e2e.remote/invoke-results",
        topic="invoke-results",
        key=invoke_result.id,
        value=invoke_result,
        value_type=InvokeResultType)
    context.send_egress(egress_message)


handler = RequestReplyHandler(functions)

#
# Serve the endpoint
#

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)


@app.route('/service', methods=['POST'])
def handle():
    response_data = handler.handle_sync(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run()
