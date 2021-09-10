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

const { Type } = require("./core");

require("./generated/types_pb");
require("./generated/request-reply_pb");

// primitive type names

const BOOLEAN_TYPENAME = "io.statefun.types/bool";
const INTEGER_TYPENAME = "io.statefun.types/int";
const FLOAT_TYPENAME = "io.statefun.types/float";
const STRING_TYPENAME = "io.statefun.types/string";

// The following types are simply not supported in JavaScript.
// TODO: should we use BigNums here?
// const LONG_TYPENAME = "io.statefun.types/long";
// const DOUBLE_TYPENAME = "io.statefun.types/double";


// noinspection JSUnresolvedVariable,JSValidateJSDoc
class TypedValueSupport {

    static TV = proto.io.statefun.sdk.reqreply.TypedValue;

    /**
     * Parse an instance of a TypedValue via the given type to a JS Object.
     *
     * @param {proto.io.statefun.sdk.reqreply.TypedValue} box
     * @param {Type} type an StateFun type
     * @returns {null|any} a JsObject that was via type or NULL if the typed value was empty.
     */
    static parseTypedValue(box, type) {
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

    static toTypedValue(obj, type) {
        if (type === undefined || type === null) {
            throw new Error("Type can not be missing");
        }
        let ret = new TypedValueSupport.TV();
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

    static toTypedValueRaw(typename, bytes) {
        let box = new TypedValueSupport.TV();
        box.setHasValue(true);
        box.setTypename(typename);
        box.setValue(bytes);
        return box;
    }

}

// primitive types

class ProtobufWrapperType extends Type {

    constructor(typename, wrapper) {
        super(typename);
        this.wrapper = wrapper;
    }

    serialize(value) {
        let w = new this.wrapper();
        w.setValue(value);
        return w.serializeBinary();
    }

    deserialize(bytes) {
        let w = this.wrapper.deserializeBinary(bytes);
        return w.getValue();
    }
}

// noinspection JSUnresolvedVariable
class BoolType extends ProtobufWrapperType {
    static INSTANCE = new BoolType();

    constructor() {
        super(BOOLEAN_TYPENAME, proto.io.statefun.sdk.types.BooleanWrapper);
    }
}

// noinspection JSUnresolvedVariable
class IntType extends ProtobufWrapperType {
    static INSTANCE = new IntType();


    constructor() {
        super(INTEGER_TYPENAME, proto.io.statefun.sdk.types.IntWrapper);
    }
}

// noinspection JSUnresolvedVariable
class FloatType extends ProtobufWrapperType {
    static INSTANCE = new FloatType();

    constructor() {
        super(FLOAT_TYPENAME, proto.io.statefun.sdk.types.FloatWrapper);
    }
}

// noinspection JSUnresolvedVariable
class StringType extends ProtobufWrapperType {
    static INSTANCE = new StringType();

    constructor() {
        super(STRING_TYPENAME, proto.io.statefun.sdk.types.StringWrapper);
    }
}


class CustomType extends Type {
    constructor(typename, serialize, deserializer) {
        super(typename);
        this._ser = serialize;
        this._desr = deserializer;
    }

    serialize(value) {
        return this._ser(value);
    }

    deserialize(bytes) {
        return this._desr(bytes);
    }
}

class JsonType extends Type {

    constructor(typename) {
        super(typename);
    }

    serialize(object) {
        return Buffer.from(JSON.stringify(object));
    }

    deserialize(buffer) {
        return JSON.parse(buffer.toString());
    }
}

class ProtobufType extends Type {
    #wrapper;

    constructor(typename, wrapper) {
        super(typename);
        this.#wrapper = wrapper;
    }

    serialize(value) {
        return value.serializeBinary();
    }

    deserialize(bytes) {
        return this.#wrapper.deserializeBinary(bytes);
    }
}

module.exports.Type = Type;

module.exports.BOOL_TYPE = BoolType.INSTANCE;
module.exports.INT_TYPE = IntType.INSTANCE;
module.exports.FLOAT_TYPE = FloatType.INSTANCE;
module.exports.STRING_TYPE = StringType.INSTANCE;

module.exports.CustomType = CustomType;
module.exports.JsonType = JsonType;
module.exports.ProtobufType = ProtobufType;
module.exports.TypedValueSupport = TypedValueSupport;