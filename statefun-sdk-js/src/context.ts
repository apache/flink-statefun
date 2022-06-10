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

import {isEmptyOrNull} from "./core";
import {Message} from "./message";
import {EgressMessage} from "./message";
import {Address} from "./core";

export class DelayedMessage {
    constructor(
        readonly delay: number,
        readonly message: Message,
        readonly token: string | undefined
    ) {}
}

export class CancellationRequest {
    constructor(readonly token: string) {
    }
}

export class InternalContext {
    caller: Address | null = null;
    readonly sent: Message[] = [];
    readonly delayed: (DelayedMessage | CancellationRequest)[] = [];
    readonly egress: EgressMessage[] = [];
}

// noinspection SuspiciousTypeOfGuard
export class Context {
    readonly #storage: any;
    readonly #self: Address;
    readonly #internalContext: InternalContext;

    /**
     * @param {Address} self an address of the currently executing function.
     * @param storage an address scoped storage.
     * @param {InternalContext} internalContext.
     */
    constructor(self: Address, storage: any, internalContext: InternalContext) {
        this.#self = self;
        this.#storage = storage;
        this.#internalContext = internalContext;
    }

    /**
     * Address Scoped Storage.
     *
     * This property represents a storage that is scoped for the currently executing function.
     * The returned object contains, as properties, the values of each registered state spec.
     *
     * @returns {*} the address scoped storage that is associated with this function.
     */
    get storage() {
        return this.#storage;
    }

    /**
     * @returns {Address | null} the caller address if this message originated from a function.
     */
    get caller(): null | Address {
        return this.#internalContext.caller;
    }

    /**
     * @returns {Address} the address of the currently executing function.
     */
    get self(): Address {
        return this.#self;
    }

    /**
     * Send a message to a function or an egress.
     *
     * @param {EgressMessage|Message} message a message to send.
     */
    send(message: Message | EgressMessage) {
        const internalContext = this.#internalContext;
        if (message instanceof EgressMessage) {
            internalContext.egress.push(message);
        } else if (message instanceof Message) {
            internalContext.sent.push(message);
        } else {
            throw new Error(`Unknown message type ${message}`);
        }
    }

    /**
     * Send a delayed message.
     *
     * @param {int} delay a number that represents a time duration in milliseconds, after it this message will be delivered.
     * @param {Message} message a message to send after the specified delay had passed.
     * @param {string} cancellationToken an optional value to associate with this message for a later cancellation.
     */
    sendAfter(delay: number, message: Message, cancellationToken?: string) {
        if (!(message instanceof Message)) {
            throw new Error(`Can only delay messages. Got ${message}`);
        }
        if (!Number.isInteger(delay)) {
            throw new Error(`delay is expected to be a number that represents a time duration in milliseconds.`);
        }
        this.#internalContext.delayed.push(new DelayedMessage(delay, message, cancellationToken));
    }

    /**
     * Cancel a delayed message (message that was sent using sendAfter) with a given token.
     * Please note that this is a best-effort operation, since the message might have been already delivered.
     * If the message was delivered, this is a no-op operation.
     * @param {string} cancellationToken
     */
    cancelDelayedMessage(cancellationToken: string) {
        if (isEmptyOrNull(cancellationToken)) {
            throw new Error(`Cancellation token can not be NULL`)
        }
        this.#internalContext.delayed.push(new CancellationRequest(cancellationToken));
    }
}



