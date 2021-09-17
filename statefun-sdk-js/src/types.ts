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

import {Type} from "./core";

import "./generated/types_pb";
import "./generated/request-reply_pb";

// primitive type names

const BOOLEAN_TYPENAME = "io.statefun.types/bool";
const INTEGER_TYPENAME = "io.statefun.types/int";
const FLOAT_TYPENAME = "io.statefun.types/float";
const STRING_TYPENAME = "io.statefun.types/string";

// The following types are simply not supported in JavaScript.
// TODO: should we use BigNums here?
// const LONG_TYPENAME = "io.statefun.types/long";
// const DOUBLE_TYPENAME = "io.statefun.types/double";

// noinspection JSValidateJSDoc
export class TypedValueSupport {

    /**
     * Parse an instance of a TypedValue via the given type to a JS Object.
     *
     * @param {proto.io.statefun.sdk.reqreply.TypedValue} box
     * @param {Type} type an StateFun type
     * @returns {null|any} a JsObject that was via type or NULL if the typed value was empty.
     */
    static parseTypedValue<T>(box: any, type: Type<T>): T | null {
        if (type === undefined || type === null) {
            throw new Error("Type can not be missing");
        }
        if (box === undefined || box === null) {
            return null;
        }
        if (!box.getHasValue()) {
            return null;
        }
        if (box.getTypename() !== type.typename) {
            throw new Error(`Type mismatch: ${box.getTypename()} is not of type ${type.typename}`);
        }
        const buf = Buffer.from(box.getValue_asU8());
        return type.deserialize(buf);
    }

    static toTypedValue<T>(obj: T, type: Type<T>): any {
        if (type === undefined || type === null) {
            throw new Error("Type can not be missing");
        }
        const ret = new proto.io.statefun.sdk.reqreply.TypedValue();
        ret.setTypename(type.typename);

        if (obj === undefined || obj === null) {
            ret.setHasValue(false);
        } else {
            ret.setHasValue(true);
            // serialize
            const buffer = type.serialize(obj);
            ret.setValue(buffer);
        }
        return ret;
    }

    static toTypedValueRaw(typename: string, bytes: Buffer | Uint8Array): proto.io.statefun.sdk.reqreply.TypedValue {
        const box = new proto.io.statefun.sdk.reqreply.TypedValue();
        box.setHasValue(true);
        box.setTypename(typename);
        box.setValue(bytes);
        return box;
    }

}

// primitive types

class ProtobufWrapperType<T> extends Type<T> {
    readonly #wrapper: any;

    constructor(typename: string, wrapper: any) {
        super(typename);
        this.#wrapper = wrapper;
    }

    serialize(value: T) {
        const wrapper = new this.#wrapper();
        wrapper.setValue(value);
        return wrapper.serializeBinary();
    }

    deserialize(bytes: Buffer): T {
        const wrapper = this.#wrapper.deserializeBinary(bytes);
        return wrapper.getValue();
    }
}

class BoolType extends ProtobufWrapperType<boolean> {
    static readonly INSTANCE = new BoolType();

    constructor() {
        super(BOOLEAN_TYPENAME, global.proto.io.statefun.sdk.types.BooleanWrapper);
    }
}

class IntType extends ProtobufWrapperType<number> {
    static readonly INSTANCE = new IntType();

    constructor() {
        super(INTEGER_TYPENAME, proto.io.statefun.sdk.types.IntWrapper);
    }
}

// noinspection JSUnresolvedVariable
class FloatType extends ProtobufWrapperType<number> {
    static readonly INSTANCE = new FloatType();

    constructor() {
        super(FLOAT_TYPENAME, proto.io.statefun.sdk.types.FloatWrapper);
    }
}

class StringType extends ProtobufWrapperType<string> {
    static readonly INSTANCE = new StringType();

    constructor() {
        super(STRING_TYPENAME, proto.io.statefun.sdk.types.StringWrapper);
    }
}

export class CustomType<T> extends Type<T> {
    readonly #serializer: (a: T) => Buffer;
    readonly #deserializer: (buf: Buffer) => T;

    constructor(typename: string, serialize: (a: T) => Buffer, deserializer: (buf: Buffer) => T) {
        super(typename);
        this.#serializer = serialize;
        this.#deserializer = deserializer;
    }

    serialize(value: T) {
        return this.#serializer(value);
    }

    deserialize(bytes: Buffer) {
        return this.#deserializer(bytes);
    }
}

export class JsonType<T> extends Type<T> {

    constructor(typename: string) {
        super(typename);
    }

    serialize(object: T) {
        return Buffer.from(JSON.stringify(object));
    }

    deserialize(buffer: Buffer): T {
        return JSON.parse(buffer.toString());
    }
}

export class ProtobufType<T> extends Type<T> {
    #wrapper: any;

    constructor(typename: string, wrapper: any) {
        super(typename);
        this.#wrapper = wrapper;
    }

    serialize(value: any): Buffer {
        return value.serializeBinary();
    }

    deserialize(bytes: Buffer) {
        return this.#wrapper.deserializeBinary(bytes);
    }
}

export const BOOL_TYPE = BoolType.INSTANCE;
export const INT_TYPE = IntType.INSTANCE;
export const FLOAT_TYPE = FloatType.INSTANCE;
export const STRING_TYPE = StringType.INSTANCE;
