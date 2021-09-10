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

const assert = require('assert');
const {TypedValueSupport} = require("../src/types");

function roundTrip(tpe, value) {
    const bytes = tpe.serialize(value);
    return tpe.deserialize(bytes);
}

describe('Simple Serialization Test', () => {
    it('should serialize a True booleans', () => {
        let actual = roundTrip(StateFun.booleanType(), true);
        assert.equal(actual, true);
    });

    it('should serialize a False booleans', () => {
        let actual = roundTrip(StateFun.booleanType(), false);
        assert.equal(actual, false);
    });

    it('should serialize a string', () => {
        let actual = roundTrip(StateFun.stringType(), "Hello world");
        assert.equal(actual, "Hello world");
    });

    it('should serialize an empty string', () => {
        let actual = roundTrip(StateFun.stringType(), "");
        assert.equal(actual, "");
    });

    it('should serialize an int', () => {
        let actual = roundTrip(StateFun.intType(), 12345);
        assert.equal(actual, 12345);
    });

    it('should serialize a float', () => {
        let actual = roundTrip(StateFun.floatType(), 123.0);
        assert.equal(actual, 123.0);
    });

    it('should serialize Json', () => {
        let actual = roundTrip(StateFun.jsonType("foo/bar"), {a: 1, b: "world"});
        assert.deepEqual(actual, {a: 1, b: "world"});
    });

    it('should round trip a TypedValue of a string.', () => {
        let box = TypedValueSupport.toTypedValue("hello", StateFun.stringType());
        let got = TypedValueSupport.parseTypedValue(box, StateFun.stringType());

        assert.deepEqual(got, "hello");
    })

    it('should box a NULL value as a missing value', () => {
        let box = TypedValueSupport.toTypedValue(null, StateFun.stringType());

        assert.equal(false, box.getHasValue());
    })

    it('Should unbox a TypedValue with a missing value as NULL', () => {
        let box = TypedValueSupport.toTypedValue(null, StateFun.stringType());
        let got = TypedValueSupport.parseTypedValue(box, StateFun.stringType());

        assert.equal(got, null);
    })

    it('Should fail to unbox with a wrong type', () => {
        let box = TypedValueSupport.toTypedValue("hello", StateFun.stringType());
        let failed = false;
        try {
            TypedValueSupport.parseTypedValue(box, StateFun.floatType());
        } catch (e) {
            failed = true;
        }
        assert.equal(failed, true);
    })
});