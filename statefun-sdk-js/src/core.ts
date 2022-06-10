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

import {Context} from "./context";
import {Message} from "./message";

/**
 * Type - represents the base class for every StateFun type.
 * each type is globally and uniquely (across languages) defined by it's Typename string (of the form <namespace>/<name>).
 */
export abstract class Type<T> {
    readonly #typename: string;

    protected constructor(typename: string) {
        validateTypeName(typename);
        this.#typename = typename;
    }

    /**
     * typename is a uniquely identifying <namespace>/<name> string that represents a value
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
     * @param value the value to serialize.
     * @returns {Buffer} the serialized value.
     */
    abstract serialize(value: T): Buffer;

    /**
     * Deserialize a previously serialized value from bytes.
     *
     * @param {Buffer} bytes a serialized value.
     * @returns a value that was serialized from the input bytes.
     */
    abstract deserialize(bytes: Buffer): T;
}

/**
 * A Stateful Function's Address.
 */
export class Address {
    constructor(readonly namespace: string, readonly name: string, readonly id: string, readonly typename: string) {
    }

    /**
     * Create an address that consist out of a namespace, name (also known as a typename) and an id.
     *
     * @param namespace the namespace part of the address
     * @param name the name part of the address
     * @param id the function's unique id.
     * @returns {Address} an address that represents a specific function instance.
     */
    static fromParts(namespace: string, name: string, id: string) {
        return new Address(namespace, name, id, `${namespace}/${name}`);
    }

    /**
     * Creates an address from a <namespace>/<name> (aka typename) and an id pair.
     * @param typename
     * @param id
     * @returns {Address}
     */
    static fromTypeNameId(typename: string, id: string) {
        if (isEmptyOrNull(id)) {
            throw new Error("id must be a defined string");
        }
        const {namespace, name} = parseTypeName(typename);
        return new Address(namespace, name, id, typename);
    }
}

/**
 * A representation of a single state value specification.
 * This is created from the following object:
 * {
 *     name: string,
 *     type: Type,
 *     expireAfterCall / expireAfterWrite : int
 * }
 */
export interface ValueSpecOpts {
    name: string;
    type: Type<any>;
    expireAfterCall?: number;
    expireAfterWrite?: number;
}

/**
 * A Stateful function instance is a two argument function that accepts a Context to preform various side-effects with,
 * and an input message.
 */
export type JsStatefulFunction = (context: Context, message: Message) => void | Promise<void>;

/**
 * A representation of a single function.
 * This can be created with the following object:
 * {
 *     typename: "foo.bar/baz",
 *     fn(context, message) {
 *         ...
 *     },
 *     specs: [..]
 * }
 */
export interface FunctionOpts {
    typename: string;
    fn: JsStatefulFunction;
    specs?: ValueSpecOpts[];
}

/**
 * an internal representation of a value spec
 */
export class ValueSpec implements ValueSpecOpts {
    constructor(
        readonly name: string,
        readonly type: Type<any>,
        readonly expireAfterCall: number = -1,
        readonly expireAfterWrite: number = -1
    ) {
    }

    /**
     * Creates a ValueSpec.
     *
     * @param {string} name the unique state name to use. Must be lowercase a-z or _.
     * @param {Type} type the statefun type to associated with this state.
     * @param {int} expireAfterCall the time-to-live (milliseconds) of this value after a call
     * @param {int} expireAfterWrite the time-to-live (milliseconds) of this value after a write
     * @returns {ValueSpec}
     */
    static fromOpts({name, type, expireAfterCall, expireAfterWrite}: ValueSpecOpts) {
        if (isEmptyOrNull(name)) {
            throw new Error("missing name");
        }
        if (!/^[_a-z]+$/.test(name)) {
            throw new Error(`a name can only contain lower or upper case letters`);
        }
        if (type === undefined || type === null) {
            throw new Error("missing type");
        }
        if (expireAfterCall != null && !Number.isInteger(expireAfterCall)) {
            throw new Error("expireAfterCall is not an integer");
        }
        if (expireAfterWrite != null && !Number.isInteger(expireAfterWrite)) {
            throw new Error("expireAfterWrite is not an integer");
        }
        return new ValueSpec(name, type, expireAfterCall, expireAfterWrite);
    }
}


/**
 * An internal representation of a function spec.
 * A function specification has a typename, a list of zero or more declared states, and an instance of a function to invoke.
 */
export class FunctionSpec implements FunctionOpts {
    constructor(
        readonly typename: string,
        readonly fn: JsStatefulFunction,
        readonly valueSpecs: ValueSpec[]
    ) {
        validateTypeName(typename);
        if (fn === undefined) {
            throw new Error(`input function must be defined.`);
        }
    }

    static fromOpts({fn, specs, typename}: FunctionOpts): FunctionSpec {
        validateTypeName(typename);
        if (fn === undefined || fn === null) {
            throw new Error(`missing function instance for ${typename}`);
        }

        const valueSpecs = (specs ?? []).map(spec => ValueSpec.fromOpts(spec));

        const seen = new Set<String>();
        for (const valueSpec of valueSpecs) {
            if (seen.has(valueSpec.name)) {
                throw new Error(`{valueSpec.name} is already defined.`);
            }

            seen.add(valueSpec.name);
        }

        return new FunctionSpec(typename, fn, valueSpecs);
    }
}

/**
 *
 * @param {string} typename a namespace/name string
 */
export function validateTypeName(typename: string) {
    parseTypeName(typename);
}

/**
 * @param {string} typename a string of  <namespace>/<name>
 * @returns {{namespace: string, name: string}}
 */
export function parseTypeName(typename: string): {namespace: string, name: string} {
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

export function isEmptyOrNull(str: string | undefined | null): boolean {
    return (str === null || str === undefined
        || (typeof (str as unknown) !== 'string') || str.length === 0);
}
