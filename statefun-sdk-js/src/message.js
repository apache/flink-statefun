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

const types = require("./types");
const {validateTypeName, Address, isEmptyOrNull} = require("./core");

class Message {
    #targetAddress;
    #typedValue;

    constructor(targetAddress, typedValue) {
        this.#targetAddress = targetAddress;
        this.#typedValue = typedValue;
    }

    get targetAddress() {
        return this.#targetAddress;
    }

    get valueTypeName() {
        return this.#typedValue.getTypename();
    }

    get rawValueBytes() {
        const tv = this.#typedValue;

        if (!tv.getHasValue()) {
            return null;
        }
        const u8 = tv.getValue_asU8();
        return Buffer.from(u8);
    }

    is(thatType) {
        return thatType.typename === this.valueTypeName;
    }

    as(tpe) {
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
    #typename;
    #box;

    constructor(typename, box) {
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
 * Constructs a Message to be sent.
 *
 * @param {string} typename a target address function type of the form <namespace>/<name> (typename).
 * @param {string} id the target address id.
 * @param {any} value a value to send.
 * @param {Type} valueType the StateFun's type of the value to send.
 * @returns {Message} an message object to be sent.
 */
function messageBuilder({typename = "", id = "", value = null, valueType = null} = {}) {
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

/**
 * Constructs an egress message to be sent.
 *
 * @param {string} typename a target address typename.
 * @param {any} value a value to send.
 * @param {Type} valueType the StateFun's type of the value to send.
 * @returns {EgressMessage} an message object to be sent.
 */
function egressMessageBuilder({typename = "", value = null, valueType = null} = {}) {
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


module.exports.Message = Message;
module.exports.EgressMessage = EgressMessage;
module.exports.messageBuilder = messageBuilder;
module.exports.egressMessageBuilder= egressMessageBuilder;