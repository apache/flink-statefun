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

import * as types from "./types";
import {validateTypeName, Address, isEmptyOrNull, Type} from "./core";

class Message {
    readonly #targetAddress: Address;
    readonly #typedValue: any;

    constructor(targetAddress: Address, typedValue: any) {
        this.#targetAddress = targetAddress;
        this.#typedValue = typedValue;
    }

    /**
     * Return this message's target address.
     */
    get targetAddress() {
        return this.#targetAddress;
    }

    /**
     * @returns {string} This message value's type name.
     */
    get valueTypeName(): string {
        return this.#typedValue.getTypename();
    }

    /**
     * @returns {Buffer} returns this message value's raw bytes.
     */
    get rawValueBytes(): Buffer {
        const tv = this.#typedValue;

        if (!tv.getHasValue()) {
            throw new Error(`Unexpected message without a value`);
        }
        const u8 = tv.getValue_asU8();
        return Buffer.from(u8);
    }

    /**
     * Check if the value carried by this message is of a given type.
     */
    is<T>(thatType: Type<T>): boolean {
        return thatType.typename === this.valueTypeName;
    }

    /**
     * Convert the raw bytes carried by this message to an object.
     *
     * @param {Type} tpe a Type that will be used to convert the raw bytes carried by this message to an object.
     */
    as<T>(tpe: Type<T>): T | null {
        const support = types.TypedValueSupport;
        return support.parseTypedValue(this.#typedValue, tpe);
    }

    isString() {
        return this.is(types.STRING_TYPE);
    }

    asString() {
        return this.as(types.STRING_TYPE);
    }

    isBoolean() {
        return this.is(types.BOOL_TYPE);
    }

    asBoolean() {
        return this.as(types.BOOL_TYPE);
    }

    isFloat() {
        return this.is(types.FLOAT_TYPE);
    }

    asFloat() {
        return this.as(types.FLOAT_TYPE);
    }

    isInt() {
        return this.is(types.INT_TYPE);
    }

    asInt() {
        return this.as(types.INT_TYPE);
    }

    get typedValue() {
        return this.#typedValue;
    }
}

class EgressMessage {
    readonly #typename;
    readonly #box;

    constructor(typename: string, box: any) {
        this.#typename = typename;
        this.#box = box;
    }

    get typename() {
        return this.#typename;
    }

    get typedValue() {
        return this.#box;
    }
}

/**
 * Message construction options
 */
export interface MessageOpts {
    typename: string;
    id: string;
    value: any;
    valueType?: Type<any> | undefined
}


/**
 * Constructs a Message to be sent.
 *
 * @param {string} typename a target address function type of the form <namespace>/<name> (typename).
 * @param {string} id the target address id.
 * @param {any} value a value to send.
 * @param {Type} valueType the StateFun's type of the value to send.
 * @returns {Message} an message object to be sent.
 */
function messageBuilder({typename = "", id = "", value = null, valueType}: MessageOpts) {
    validateTypeName(typename);
    if (isEmptyOrNull(id)) {
        throw new Error("Target id (id) can not missing");
    }
    if (value === undefined || value === null) {
        throw new Error("Missing value");
    }
    if (valueType === null || valueType === undefined) {
        if (typeof value === 'string') {
            valueType = types.STRING_TYPE;
        } else if (Number.isSafeInteger(value)) {
            valueType = types.INT_TYPE;
        } else {
            throw new Error(`Missing valueType. Please provide a specific value type for ${value}.`);
        }
    }
    const box = types.TypedValueSupport.toTypedValue(value, valueType);
    return new Message(Address.fromTypeNameId(typename, id), box);
}

export interface EgressOpts {
    typename: string;
    value: any;
    valueType?: Type<any>;
}

/**
 * Constructs an egress message to be sent.
 *
 * @param {string} typename a target address typename.
 * @param {any} value a value to send.
 * @param {Type} valueType the StateFun's type of the value to send.
 * @returns {EgressMessage} an message object to be sent.
 */
function egressMessageBuilder({typename, value, valueType}: EgressOpts) {
    validateTypeName(typename);
    if (value === undefined || value === null) {
        throw new Error("Missing value");
    }
    if (valueType === null || valueType === undefined) {
        throw new Error("Missing type.");
    }
    const box = types.TypedValueSupport.toTypedValue(value, valueType);
    return new EgressMessage(typename, box);
}


export {Message}
export {EgressMessage}
export {messageBuilder}
export {egressMessageBuilder}