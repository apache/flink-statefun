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

import {describe, expect} from '@jest/globals'
import {StateFun} from "../src/statefun";
import {TypedValueSupport} from "../src/types";
import {egressMessageBuilder, Message, messageBuilder} from "../src/message";
import {Address} from "../src/core";

// test constants
const UserType = StateFun.jsonType("io.foo.bar/User");
const aUser = {name: "bob", last: "mop"};
const aUserBytes = UserType.serialize(aUser);
const anAddress = Address.fromTypeNameId("io.foo.bar/Greeter", "bob");
const aUserTypedValue = TypedValueSupport.toTypedValue(aUser, UserType);


describe('Message Test', () => {
    it('Should set the basic properties', () => {

        const msg = new Message(anAddress, aUserTypedValue);

        expect(msg.targetAddress).toStrictEqual(anAddress);
        expect(msg.valueTypeName).toStrictEqual(UserType.typename);
        expect(msg.rawValueBytes).toStrictEqual(aUserBytes);
    });

    it('Should recognize a custom type', () => {
        const msg = new Message(anAddress, aUserTypedValue);

        expect(msg.is(UserType)).toStrictEqual(true);
        expect(msg.is(StateFun.intType())).toStrictEqual(false);
    });

    it('Should deserialize a custom type', () => {
        const msg = new Message(anAddress, aUserTypedValue);
        const gotUser = msg.as(UserType);

        expect(gotUser).toStrictEqual(aUser);
    });

    it('Should deserialize a string type', () => {
        const tpe = StateFun.stringType();

        const msg = new Message(anAddress, TypedValueSupport.toTypedValue("Hello there", tpe));

        expect(msg.isString()).toStrictEqual(true);
        expect(msg.asString()).toStrictEqual("Hello there");
    });

    it('Should deserialize a boolean type', () => {
        const tpe = StateFun.booleanType();

        const msg = new Message(anAddress, TypedValueSupport.toTypedValue(true, tpe));

        expect(msg.isBoolean()).toStrictEqual(true);
        expect(msg.asBoolean()).toStrictEqual(true);
    });

    it('Should deserialize a float type', () => {
        const tpe = StateFun.floatType();

        const msg = new Message(anAddress, TypedValueSupport.toTypedValue(1.0, tpe));

        expect(msg.isFloat()).toStrictEqual(true);
        expect(msg.asFloat()).toStrictEqual(1.0);
    });

    it('Message builder should construct a correct message', () => {
        const msg = messageBuilder({
            typename: "foo/bar",
            id: "1",
            value: 2,
        });

        expect(msg.targetAddress).toStrictEqual(Address.fromTypeNameId("foo/bar", "1"));
        expect(msg.isInt()).toStrictEqual(true);
        expect(msg.asInt()).toStrictEqual(2);
    });

    it('Egress message builder should construct a correct egress message', () => {
        const msg = egressMessageBuilder({typename: "foo/bar", value: 123, valueType: StateFun.floatType()});
        expect(msg.typename).toStrictEqual("foo/bar");

        const actual = TypedValueSupport.parseTypedValue(msg.typedValue, StateFun.floatType());
        expect(actual).toStrictEqual(123);
    });
});
