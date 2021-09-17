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

import "./generated/request-reply_pb";
import {TypedValueSupport} from "./types";
import {Type, ValueSpec} from "./core";

const MutationType = proto.io.statefun.sdk.reqreply.FromFunction.PersistedValueMutation.MutationType;

// noinspection JSValidateJSDoc
export class Value<T> {
    readonly name: string;
    readonly #type: Type<T>;
    #box: proto.io.statefun.sdk.reqreply.TypedValue | null;
    #mutated: boolean;
    #deleted: boolean;

    /**
     *
     * @param {string} name
     * @param {Type} type
     * @param {proto.io.statefun.sdk.reqreply.TypedValue} box
     */
    constructor(name: string, type: Type<T>, box: proto.io.statefun.sdk.reqreply.TypedValue | null) {
        this.name = name;
        this.#type = type;
        this.#box = box;
        this.#mutated = false;
        this.#deleted = false;
    }

    getValue(): T | null {
        if (this.#deleted) {
            return null;
        }
        return TypedValueSupport.parseTypedValue(this.#box, this.#type);
    }

    setValue(jsObject: T | null) {
        if (jsObject === undefined || jsObject === null) {
            this.#mutated = true;
            this.#deleted = true;
            this.#box = null;
        } else {
            this.#mutated = true;
            this.#deleted = false;
            this.#box = TypedValueSupport.toTypedValue(jsObject, this.#type);
        }
    }

    // internal helpers

    asMutation(): proto.io.statefun.sdk.reqreply.FromFunction.PersistedValueMutation | null {
        if (!this.#mutated) {
            return null;
        }
        const mutation = new proto.io.statefun.sdk.reqreply.FromFunction.PersistedValueMutation();
        mutation.setStateName(this.name);
        if (this.#deleted) {
            mutation.setMutationType(MutationType['DELETE']);
        } else {
            mutation.setMutationType(MutationType['MODIFY']);
            mutation.setStateValue(this.#box);
        }
        return mutation;
    }

    static fromState<U>(persistedValue: proto.io.statefun.sdk.reqreply.ToFunction.PersistedValue, type: Type<U>) {
        const name = persistedValue.getStateName();
        return new Value<U>(name, type, persistedValue.getStateValue());
    }
}

// noinspection JSValidateJSDoc
export class AddressScopedStorageFactory {

    /**
     * Tries to create an AddressScopedStorage. An object that contains each known state as a property on that object.
     *
     * @param {proto.io.statefun.sdk.reqreply.ToFunction.InvocationBatchRequest} invocationBatchRequest
     * @param { [ValueSpec] } knownStates
     * @returns either a list of missing ValueSpecs or a list of Values and an AddressScopedStorage.
     */
    static tryCreateAddressScopedStorage(
        invocationBatchRequest: proto.io.statefun.sdk.reqreply.ToFunction.InvocationBatchRequest,
        knownStates: ValueSpec[]
    ) {
        const receivedState = AddressScopedStorageFactory.indexActualState(invocationBatchRequest);
        const {found, missing} = AddressScopedStorageFactory.extractKnownStates(knownStates, receivedState);
        if (missing.length > 0) {
            // the caller needs to respond with an IncompleteInvocationResponse,
            // that contains all the missing specs.
            return {
                missing: missing,
                values: null,
                storage: null
            }
        }
        // TODO: consider caching and setting the newly received states by calling setValue on each individual value.
        const storage = AddressScopedStorageFactory.create(found);
        return {
            missing: null,
            values: found,
            storage: storage,
        };
    }

    static extractKnownStates(knownStates: ValueSpec[], receivedState: Record<string, any>) {
        const found = [];
        const missing = [];
        for (const spec of knownStates) {
            if (!receivedState.hasOwnProperty(spec.name)) {
                missing.push(spec);
                continue;
            }
            const persistedValue = receivedState[spec.name];
            found.push(Value.fromState(persistedValue, spec.type));
        }
        return {found, missing};
    }

    static indexActualState(
        batchRequest: proto.io.statefun.sdk.reqreply.ToFunction.InvocationBatchRequest
    ): Record<string, any> {
        const states = batchRequest.getStateList();
        const gotState: Record<string, any> = {};
        for (const state of states) {
            gotState[state.getStateName()] = state;
        }
        return gotState;
    }

    /**
     * @param {[Value]} values a list of initialize values
     */
    static create(values: Value<unknown>[]) {
        const storage = Object.create(null);
        for (const v of values) {
            Object.defineProperty(storage, v.name, {
                get: () => v.getValue(),
                set: (newValue) => v.setValue(newValue)
            })
        }
        return Object.seal(storage);
    }

    static collectMutations(values: Value<unknown>[]): proto.io.statefun.sdk.reqreply.FromFunction.PersistedValueMutation[] {
        return <proto.io.statefun.sdk.reqreply.FromFunction.PersistedValueMutation[]>values
            .map(value => value.asMutation())
            .filter(mutation => mutation !== null);
    }
}
