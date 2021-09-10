/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"use strict";

const {kafkaEgressMessage} = require("./statefun");
const http = require("http");

const {StateFun, Message, Context, messageBuilder} = require("./statefun");

// ------------------------------------------------------------------------------------------------------
// Greeter
// ------------------------------------------------------------------------------------------------------

/**
 * Greeter.
 *
 * @param {Context} context a StateFun context.
 * @param {Message} message the input message.
 * @returns {Promise<void>}
 */
async function greeter(context, message) {
    const who = context.self.id;
    const inputMessage = message.asString();

    console.log(`received a message ${inputMessage}`);

    context.storage.seen += 1;

    const greeting = await computeGreeting(who, context.storage.seen);

    context.send(messageBuilder({
        typename: "fns/mailer",
        id: who,
        value: greeting
    }));

    context.sendAfter(1_000, messageBuilder({
        typename: "fns/mailer",
        id: who,
        value: `Just making sure you've got my previous greeting`
    }));
}

async function computeGreeting(who, seen) {
    return `Hello ${who} I've seen you ${seen} times!`;
}

// ------------------------------------------------------------------------------------------------------
// Bind all the functions
// ------------------------------------------------------------------------------------------------------

let statefun = new StateFun();

statefun.bind({
    typename: "fns/greeter",
    fn: greeter,
    specs: [
        {
            name: "seen",
            type: StateFun.intType(),
        },
        {
            name: "dummy",
            type: StateFun.stringType(),
            expireAfterCall: 1_000 * 60 * 60 // one hour
        },
        {
            name: "last_login",
            type: StateFun.jsonType("events/Login"),
            expireAfterWrite: 1_000 * 60
        }
    ]
});

statefun.bind({
    typename: "fns/mailer",
    async fn(context, message) {
        const who = context.self.id;
        const email = message.asString();

        context.send(kafkaEgressMessage({
            typename: "egress/kafka",
            key: who,
            topic: "out_emails",
            value: email
        }));
    }
});


// ------------------------------------------------------------------------------------------------------
// Serve all the functions
// ------------------------------------------------------------------------------------------------------

http.createServer(statefun.handler()).listen(8000);


