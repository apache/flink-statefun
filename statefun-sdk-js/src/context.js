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
'use strict';

const {isEmptyOrNull} = require("./core");
const {Message} = require("./message");
const {EgressMessage} = require("./message");
const {Address} = require("./core");

// noinspection JSUnusedGlobalSymbols
class Context {
    #storage;
    #self;
    #caller;
    #sent;
    #delayed;
    #egress;

    /**
     * @param {Address} self an address of the currently executing function.
     * @param {Address | null} caller an optional caller address.
     * @param storage an address scoped storage.
     */
    constructor(self, caller, storage) {
        this.#self = self;
        this.#storage = storage;
        this.#caller = caller;
        this.#sent = [];
        this.#delayed = [];
        this.#egress = [];
    }

    /**
     * Address Scoped Storage.
     *
     * This property represents a storage that is scoped for the currently executing function.
     * The returned object contains, as properties, the values of each registered state spec.
     *
     * @returns {any} the address scoped storage that is associated with this function.
     */
    get storage() {
        return this.#storage;
    }

    /**
     * @returns {Address | null} the caller address if this message originated from a function.
     */
    get caller() {
        return this.#caller;
    }

    /**
     * @returns {Address} the address of the currently executing function.
     */
    get self() {
        return this.#self;
    }

    /**
     * Send a message to a function or an egress.
     *
     * @param {EgressMessage|Message} message a message to send.
     */
    send(message) {
        if (message instanceof EgressMessage) {
            this.#egress.push(message);
        } else if (message instanceof Message) {
            this.#sent.push(message);
        } else {
            throw new Error(`Unknown message type ${message}`);
        }
    }

    /**
     * Send a delayed message.
     *
     * @param {number} delay a number that represents a time duration in milliseconds, after it this message will be delivered.
     * @param {Message} message a message to send after the specified delay had passed.
     * @param {string} cancellationToken an optional value to associate with this message for a later cancellation.
     */
    sendAfter(delay, message, cancellationToken = undefined) {
        if (!(message instanceof Message)) {
            throw new Error(`Can only delay messages. Got ${message}`);
        }
        if (!Number.isInteger(delay)) {
            throw new Error(`delay is expected to be a number that represents a time duration in milliseconds.`);
        }
        if (isEmptyOrNull(cancellationToken)) {
            this.#delayed.push({type: 'send', delay: delay, what: message});
        } else {
            this.#delayed.push({type: 'send_token', delay: delay, token: cancellationToken, what: message});
        }
    }

    /**
     * Cancel a delayed message (message that was sent using sendAfter) with a given token.
     * Please note that this is a best-effort operation, since the message might have been already delivered.
     * If the message was delivered, this is a no-op operation.
     * @param {string} cancellationToken
     */
    cancelDelayedMessage(cancellationToken) {
        if (isEmptyOrNull(cancellationToken)) {
            throw new Error(`Cancellation token can not be NULL`)
        }
        this.#delayed.push({type: 'cancel', token: cancellationToken});
    }

    // ---------------------------------------------------------------------------------------------------
    // Internal
    // ---------------------------------------------------------------------------------------------------

    set caller(newCaller) {
        this.#caller = newCaller;
    }

    get sent() {
        return this.#sent;
    }

    get egresses() {
        return this.#egress;
    }

    get delayed() {
        return this.#delayed;
    }
}

module.exports.Context = Context;
