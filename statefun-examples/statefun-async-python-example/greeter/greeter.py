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

from statefun import *
import asyncio

functions = StatefulFunctions()

@functions.bind(
    typename="example/greeter",
    specs=[ValueSpec(name='seen_count', type=IntType)])
async def greet(context, greet_request):
    storage = context.storage

    seen = storage.seen_count
    if not seen:
        seen = 1
    else:
        seen += 1
    storage.seen_count = seen

    who = context.address.id # the person name whom we want to great, is the id part of our address.
    response = await compute_greeting(who, seen)
    egress_message = kafka_egress_message(typename="example/greets", topic="greetings", key=who, value=response)
    context.send_egress(egress_message)


async def compute_greeting(name, seen):
    """
    Compute a personalized greeting, based on the number of times this @name had been seen before.
    """
    templates = ["", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"]
    if seen < len(templates):
        greeting = templates[seen] % name
    else:
        greeting = f"Nice to see you at the {seen}-nth time {name}!"

    await asyncio.sleep(1)
    return greeting


handler = RequestReplyHandler(functions)

#
# Serve the endpoint
#

from aiohttp import web

handler = RequestReplyHandler(functions)

async def handle(request):
    req = await request.read()
    res = await handler.handle_async(req)
    return web.Response(body=res, content_type="application/octet-stream")

app = web.Application()
app.add_routes([web.post('/statefun', handle)])

if __name__ == '__main__':
    web.run_app(app, port=8000)
