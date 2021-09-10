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

const {StateFun} = require('../src/statefun');
const {kinesisEgressMessage, kafkaEgressMessage} = require("../src/egress");
const {trySerializerForEgress} = require("../src/egress");

const assert = require('assert');


describe('Egress', () => {

    it('Kafka egress builder should construct a correct record', () => {
        let egress = kafkaEgressMessage({
            typename: "foo/bar",
            topic: "greets",
            value: "hello world"
        });

        assert.deepEqual(egress.typename, "foo/bar");
        assert.deepEqual(egress.typedValue.getTypename(), "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        assert.deepEqual(egress.typedValue.getHasValue(), true);
        assert.deepEqual(egress.typedValue.getValue_asU8().length > 0, true);
    });

    it('Kafka egress builder should use a type if specified', () => {
        let used = false;
        let egress = kafkaEgressMessage({
            typename: "foo/bar",
            topic: "greets",
            key: "key",
            value: {a: "a", b: "b"},
            valueType: {
                // simulate an inline custom type with a serialize method.
                serialize(what) {
                    used = true;
                    return Buffer.from(JSON.stringify(what));
                }
            }
        });

        assert.deepEqual(used, true);
        assert.deepEqual(egress.typename, "foo/bar");
        assert.deepEqual(egress.typedValue.getTypename(), "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        assert.deepEqual(egress.typedValue.getHasValue(), true);
        assert.deepEqual(egress.typedValue.getValue_asU8().length > 0, true);
    });

    it('Kinesis egress builder should use a type if specified', () => {
        let used = false;
        let egress = kinesisEgressMessage({
            typename: "foo/bar",
            stream: "greets",
            partitionKey: "hello",
            value: {a: "a", b: "b"},
            valueType: {
                // simulate an inline custom type with a serialize method.
                serialize(what) {
                    used = true;
                    return Buffer.from(JSON.stringify(what));
                }
            }
        });

        assert.deepEqual(used, true);
        assert.deepEqual(egress.typename, "foo/bar");
        assert.deepEqual(egress.typedValue.getTypename(), "type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord");
        assert.deepEqual(egress.typedValue.getHasValue(), true);
        assert.deepEqual(egress.typedValue.getValue_asU8().length > 0, true);
    });

    it('Kinesis egress builder should construct a correct record', () => {
        let egress = kinesisEgressMessage({
            typename: "foo/bar",
            stream: "greets",
            value: JSON.stringify({a: "a", b: "b"}),
            valueType: StateFun.jsonType("foo/bar")
        });

        assert.deepEqual(egress.typename, "foo/bar");
        assert.deepEqual(egress.typedValue.getTypename(), "type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord");
        assert.deepEqual(egress.typedValue.getHasValue(), true);
        assert.deepEqual(egress.typedValue.getValue_asU8().length > 0, true);
    });

    it('Should deduce a UTF8 string type', () => {
        let bytes = trySerializerForEgress(null, "hello world");
        assert.deepEqual(bytes instanceof Uint8Array, true);
        assert.deepEqual(bytes.toString(), "hello world");
    });

    it('Should accept raw bytes', () => {
        let bytes = trySerializerForEgress(null, Buffer.from("hello world"));
        assert.deepEqual(bytes instanceof Uint8Array, true);
        assert.deepEqual(bytes.toString(), "hello world");
    });

    it('Should accept Uint8Array', () => {
        let arr = new Uint8Array([1, 2, 3, 4]);
        let bytes = trySerializerForEgress(null, arr);
        assert.deepEqual(bytes instanceof Uint8Array, true);
        assert.deepEqual(bytes, arr);
    });

    it('Should serializer an integer as a 32 bit big endian', () => {
        let bytes = trySerializerForEgress(null, 1234567);
        assert.deepEqual(bytes instanceof Uint8Array, true);
        assert.deepEqual(bytes.readInt32BE(0), 1234567);
    });

    it('Should NOT try to automatically deserialize floats', () => {
        let failed = false;
        try {
            trySerializerForEgress(null, 1.5);
        } catch (e) {
            failed = true;
        }
        assert.deepEqual(failed, true);
    });

    it('Should use the custom type if one is provided', () => {
        const UserType = StateFun.jsonType("io.foo.bar/User");
        const aUser = {name: "bob", last: "mop"};

        const bytes = trySerializerForEgress(UserType, aUser);
        const actual = UserType.deserialize(bytes);

        assert.deepEqual(actual, aUser);
    });
});