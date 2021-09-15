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

import "./generated/kafka-egress_pb";
import "./generated/kinesis-egress_pb";

import {Type, validateTypeName} from "./core";

import {isEmptyOrNull} from "./core";
import {TypedValueSupport} from "./types";
import {EgressMessage} from "./message";

// noinspection JSUnresolvedVariable
const PB_KAFKA = global.proto.io.statefun.sdk.egress.KafkaProducerRecord;

// noinspection JSUnresolvedVariable
const PB_KINESIS = global.proto.io.statefun.sdk.egress.KinesisEgressRecord;

function serialize(type: Type<any> | undefined, value: any): Buffer {
    if (!(type === undefined || type === null)) {
        return type.serialize(value);
    }
    if (typeof value === 'string') {
        return Buffer.from(value);
    }
    if (Number.isSafeInteger(value)) {
        let buf = Buffer.alloc(4);
        buf.writeInt32BE(value);
        return buf;
    }
    if (value instanceof Uint8Array) {
        // this includes node's Buffer.
        return Buffer.from(value);
    }
    throw new Error("Unable to deduce a type automatically. Please provide an explicit type, or string/Buffer as a value.");
}

export interface KafkaEgressOpts {
    typename: string,
    topic: string,
    key?: string,
    value: any,
    valueType?: Type<any>
}

/**
 * Build a message that can be emitted to a Kafka generic egress.
 *
 * @param typename
 * @param topic
 * @param key
 * @param value
 * @param valueType
 * @returns {EgressMessage}
 */
function kafkaEgressMessage({typename = "", topic = "", key = "", value = null, valueType}: KafkaEgressOpts) {
    if (isEmptyOrNull(typename)) {
        throw new Error("typename is missing");
    }
    validateTypeName(typename);
    if (isEmptyOrNull(topic)) {
        throw new Error("topic is missing")
    }
    if (value === undefined || value === null) {
        throw new Error("value is missing");
    }
    let pbKafka = new PB_KAFKA()
    pbKafka.setTopic(topic);
    pbKafka.setValueBytes(serialize(valueType, value));
    if (!isEmptyOrNull(key)) {
        pbKafka.setKey(key);
    }
    const bytes = pbKafka.serializeBinary();
    const box = TypedValueSupport.toTypedValueRaw("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord", bytes);
    return new EgressMessage(typename, box);
}

export interface KinesisEgressOpts {
    typename: string;
    stream: string,
    partitionKey: string,
    hashKey?: string;
    value: any;
    valueType?: Type<any>
}

function kinesisEgressMessage({typename = "", stream = "", partitionKey = "", hashKey = "", value = null, valueType}: KinesisEgressOpts) {
    if (isEmptyOrNull(typename)) {
        throw new Error("typename is missing");
    }
    validateTypeName(typename);
    if (isEmptyOrNull(stream)) {
        throw new Error("stream is missing");
    }
    if (isEmptyOrNull(partitionKey)) {
        throw new Error("partition key is missing");
    }
    if (value === undefined || value === null) {
        throw new Error("value is missing");
    }
    let record = new PB_KINESIS();
    record.setStream(stream);
    record.setPartitionKey(partitionKey);
    record.setValueBytes(serialize(valueType, value))
    if (!isEmptyOrNull(hashKey)) {
        record.setExplicitHashKey(hashKey);
    }
    const bytes = record.serializeBinary();
    const box = TypedValueSupport.toTypedValueRaw("type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord", bytes);
    return new EgressMessage(typename, box);
}

export {kafkaEgressMessage}
export {kinesisEgressMessage}
export {serialize as trySerializerForEgress}