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

/**
 * Type - represents the base class for every StateFun type.
 * each type is globally and uniquely (across languages) defined by it's Typename string (of the form <namespace>/<name>).
 */
class Type {
    #typename;

    constructor(typename) {
        validateTypeName(typename);
        this.#typename = typename;
    }

    /**
     * typename is a uniquely identifying <namespace>/<name> string that presents a value
     * in StateFun's type system.
     *
     * @returns {string} the typename representation of this type.
     */
    get typename() {
        return this.#typename;
    }

    /**
     * Serialize a value to bytes.
     *
     * @param value
     * @returns {Buffer} the serialized value.
     */
    serialize(value) {
        throw new Error('Unimplemented method');
    }

    /**
     * Deserialize a previously serialized value from bytes.
     *
     * @param {Buffer} bytes a serialized value.
     * @returns a value that was serialized from the input bytes.
     */
    deserialize(bytes) {
        throw new Error('Unimplemented method');
    }
}

/**
 * A Stateful Function's Address.
 */
class Address {
    #namespace;
    #name;
    #id;
    #typename;

    constructor(namespace, name, id, typename) {
        this.#namespace = namespace;
        this.#name = name
        this.#id = id;
        this.#typename = typename;
    }

    static fromParts(namespace, name, id) {
        return new Address(namespace, name, id, `${namespace}/${name}`);
    }

    // noinspection DuplicatedCode
    static fromTypeNameId(typename, id) {
        if (isEmptyOrNull(id)) {
            throw new Error("id must be a defined string");
        }
        const {namespace, name} = parseTypeName(typename);
        return new Address(namespace, name, id, typename)
    }


    /**
     * @returns {string} returns the type name string (typename) of this function.
     */
    get typename() {
        return this.#typename;
    }

    /**
     * @returns {string} the id part of the address.
     */
    get id() {
        return this.#id;
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    get namespace() {
        return this.#namespace;
    }

    get name() {
        return this.#name;
    }
}

class ValueSpec {
    #name;
    #type;
    #expireAfterCall;
    #expireAfterWrite;

    constructor(name, type, expireAfterCall, expireAfterWrite) {
        this.#name = name;
        this.#type = type;
        this.#expireAfterCall = expireAfterCall;
        this.#expireAfterWrite = expireAfterWrite;
    }

    static fromObj({name = "", type = null, expireAfterCall = -1, expireAfterWrite = -1} = {}) {
        if (isEmptyOrNull(name)) {
            throw new Error("missing name");
        }
        if (!/^[_a-z]+$/.test(name)) {
            throw new Error(`a name can only contain lower or upper case letters`);
        }
        if (type === undefined || type === null) {
            throw new Error("missing type");
        }
        if (!Number.isInteger(expireAfterCall)) {
            throw new Error("expireAfterCall is not an integer");
        }
        if (!Number.isInteger(expireAfterWrite)) {
            throw new Error("expireAfterWrite is not an integer");
        }
        return new ValueSpec(name, type, expireAfterCall, expireAfterWrite);
    }

    /**
     *
     * @returns {string} the name of the this spec
     */
    get name() {
        return this.#name;
    }

    /**
     * @returns {Type} this StateFun type.
     */
    get type() {
        return this.#type;
    }

    get expireAfterWrite() {
        return this.#expireAfterWrite;
    }

    get expireAfterCall() {
        return this.#expireAfterCall;
    }
}

class FunctionSpec {
    #typename;
    #fn;
    #valueSpecs;

    constructor(typename, fn, specs) {
        validateTypeName(typename);
        if (fn === undefined) {
            throw new Error(`input function must be defined.`);
        }
        this.#typename = typename;
        this.#fn = fn;
        this.#valueSpecs = specs;
    }

    get valueSpecs() {
        return this.#valueSpecs;
    }

    get fn() {
        return this.#fn;
    }
}

/**
 *
 * @param {string} typename a namespace/name string
 */
function validateTypeName(typename) {
    parseTypeName(typename);
}

/**
 * @param {string} typename a string of  <namespace>/<name>
 * @returns {{namespace: string, name: string}}
 */
function parseTypeName(typename) {
    if (isEmptyOrNull(typename)) {
        throw new Error(`typename must be provided and of the form <namespace>/<name>`);
    }
    const index = typename.lastIndexOf("/");
    if (index < 0 || index > typename.length) {
        throw new Error(`Unable to find a / in ${typename}`);
    }
    const namespace = typename.substring(0, index);
    const name = typename.substring(index + 1);
    if (namespace === undefined || namespace.length === 0 || name === undefined || name.length === 0) {
        throw new Error(`Illegal ${typename}, it must be of a form <namespace>/<name>`);
    }
    return {namespace, name};
}

function isEmptyOrNull(s) {
    return (s === null || s === undefined || (typeof s != 'string') || s.length === 0);
}

module.exports.FunctionSpec = FunctionSpec;
module.exports.ValueSpec = ValueSpec;
module.exports.Address = Address;
module.exports.Type = Type;
module.exports.validateTypeName = validateTypeName;
module.exports.parseTypeName = parseTypeName;
module.exports.isEmptyOrNull = isEmptyOrNull;
