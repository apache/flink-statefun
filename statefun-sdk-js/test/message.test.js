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

const {StateFun, Message, Address, messageBuilder} = require('../src/statefun');
const {TypedValueSupport} = require("../src/types");
const assert = require('assert');
const {egressMessageBuilder} = require("../src/message");

// test constants
const UserType = StateFun.jsonType("io.foo.bar/User");
const aUser = {name: "bob", last: "mop"};
const aUserBytes = UserType.serialize(aUser);
const anAddress = new Address("io.foo.bar/Greeter", "bob");
const aUserTypedValue = TypedValueSupport.toTypedValue(aUser, UserType);


describe('Message Test', () => {
    it('Should set the basic properties', () => {

        const msg = new Message(anAddress, aUserTypedValue);

        assert.deepEqual(msg.targetAddress, anAddress);
        assert.deepEqual(msg.valueTypeName, UserType.typename);
        assert.deepEqual(msg.rawValueBytes, aUserBytes);
    });

    it('Should recognize a custom type', () => {
        const msg = new Message(anAddress, aUserTypedValue);

        assert.equal(msg.is(UserType), true);
        assert.equal(msg.is(StateFun.intType()), false);
    });

    it('Should deserialize a custom type', () => {
        const msg = new Message(anAddress, aUserTypedValue);
        const gotUser = msg.as(UserType);

        assert.deepEqual(gotUser, aUser);
    });

    it('Should deserialize a string type', () => {
        const tpe = StateFun.stringType();

        const msg = new Message(anAddress, TypedValueSupport.toTypedValue("Hello there", tpe));

        assert.equal(msg.isString(), true);
        assert.deepEqual(msg.asString(), "Hello there");
    });

    it('Should deserialize a boolean type', () => {
        const tpe = StateFun.booleanType();

        const msg = new Message(anAddress, TypedValueSupport.toTypedValue(true, tpe));

        assert.equal(msg.isBoolean(), true);
        assert.deepEqual(msg.asBoolean(), true);
    });

    it('Should deserialize a float type', () => {
        const tpe = StateFun.floatType();

        const msg = new Message(anAddress, TypedValueSupport.toTypedValue(1.0, tpe));

        assert.equal(msg.isFloat(), true);
        assert.deepEqual(msg.asFloat(), 1.0);
    });

    it('Message builder should construct a correct message', () => {
        const msg = messageBuilder({
            typename: "foo/bar",
            id: "1",
            value: 2,
        });
        assert.deepEqual(msg.targetAddress, new Address("foo/bar", "id"));
        assert.equal(msg.isInt(), true);
        assert.deepEqual(msg.asInt(), 2);
    });

    it('Egress message builder should construct a correct egress message', () => {
        const msg = egressMessageBuilder({typename: "foo/bar", value: 123, valueType: StateFun.floatType()});
        assert.deepEqual(msg.typename, "foo/bar");

        const actual = TypedValueSupport.parseTypedValue(msg.typedValue, StateFun.floatType());
        assert.deepEqual(actual, 123);
    });
});