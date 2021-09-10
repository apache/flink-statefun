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
const {StateFun} = require('../src/statefun');
const {Value, AddressScopedStorageFactory} = require('../src/storage');
const {TypedValueSupport} = require("../src/types");
const _ = require("../src/generated/request-reply_pb");
const assert = require('assert');

function stateFrom(name, tpe, obj) {
    let pv = new proto.io.statefun.sdk.reqreply.ToFunction.PersistedValue();
    pv.setStateName(name);
    pv.setStateValue(TypedValueSupport.toTypedValue(obj, tpe))
    return pv;
}

describe('Value Test', () => {
    it('should demonstrate a simple usage', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let v = Value.fromState(incomingState, incomingType);

        assert.deepEqual(v.getValue(), 123);

        v.setValue(v.getValue() + 1);
        assert.deepEqual(v.getValue(), 124);

        v.setValue(null);
        assert.equal(v.getValue(), null);

        v.setValue(5);
        assert.deepEqual(v.getValue(), 5);
    });

    it('should round trip successfully', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let mutation;
        {
            let v = Value.fromState(incomingState, incomingType);
            v.setValue(v.getValue() + 1) // value should be 124
            mutation = v.asMutation();
        }

        assert.deepEqual(mutation.getStateName(), "seen");
        assert.deepEqual(mutation.getMutationType(), 1);

        const actual = TypedValueSupport.parseTypedValue(mutation.getStateValue(), incomingType);

        assert.deepEqual(actual, 124);
    });

    it('should not produce a mutation if nothing has changed.', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let mutation;
        {
            let v = Value.fromState(incomingState, incomingType);
            // do nothing
            mutation = v.asMutation();
        }

        assert.equal(mutation, null);
    });

    it('should produce mutation of type DELETE', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let mutation;
        {
            let v = Value.fromState(incomingState, incomingType);

            v.setValue(null); // acts as delete.

            mutation = v.asMutation();
        }

        assert.deepEqual(mutation.getMutationType(), 0);
    });


    it('AddressScopedStorageFactory should produce an object with registered states as attributes', () => {
        const incomingType = StateFun.intType();
        let incomingState1 = stateFrom("seen", incomingType, 123);
        let incomingState2 = stateFrom("idle", incomingType, 456);

        let v1 = Value.fromState(incomingState1, incomingType);
        let v2 = Value.fromState(incomingState2, incomingType);

        let mutations;
        {
            let storage = AddressScopedStorageFactory.create([v1, v2]);

            storage.seen += 1;
            storage.idle += 1;

            mutations = AddressScopedStorageFactory.collectMutations([v1, v2]);
        }

        assert.deepEqual(mutations.length, 2);
    });
});