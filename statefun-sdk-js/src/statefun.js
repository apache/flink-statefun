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

const {validateTypeName, FunctionSpec, ValueSpec, Address} = require("./core");
const {Context} = require("./context");
const {Message, message_builder, egress_message_builder} = require("./message");
const {kafka_egress_message, kinesis_egress_message} = require("./egress");
const {handle} = require("./handler");

const {BOOL_TYPE, CustomType, FLOAT_TYPE, INT_TYPE, JsonType, ProtobufType, STRING_TYPE} = require("./types");

class StateFun {
    #fns;

    constructor() {
        this.#fns = {};
    }

    bind({typename = "", fn = null, specs = []} = {}) {
        validateTypeName(typename);
        if (fn === undefined || fn === null) {
            throw new Error(`missing function instance for ${typename}`);
        }
        let validatedSpecs = [];
        let seen = {};
        for (let spec of specs) {
            const valueSpec = ValueSpec.fromObj(spec);
            if (seen.hasOwnProperty(valueSpec.name)) {
                throw new Error(`${valueSpec.name} is already defined.`);
            }
            seen[valueSpec.name] = valueSpec;
            validatedSpecs.push(valueSpec);
        }
        this.#fns[typename] = new FunctionSpec(typename, fn, validatedSpecs);
    }

    // type constructors

    static intType() {
        return INT_TYPE;
    }

    static stringType() {
        return STRING_TYPE;
    }

    static booleanType() {
        return BOOL_TYPE;
    }

    static floatType() {
        return FLOAT_TYPE;
    }

    static jsonType(typename) {
        return new JsonType(typename);
    }

    static protoType(typename, googleProtobufGeneratedType) {
        return new ProtobufType(typename, googleProtobufGeneratedType);
    }

    static customType(typename, serialize, deserializer) {
        return new CustomType(typename, serialize, deserializer);
    }

    handler() {
        const self = this;
        return async (req, res) => {
            await self.handle(req, res);
        }
    }

    async handle(req, res) {
        let outBuf;
        try {
            const chunks = [];
            for await (const chunk of req) {
                chunks.push(chunk);
            }
            const inBuf = Buffer.concat(chunks);
            outBuf = await handle(inBuf, this.#fns);
        } catch (e) {
            console.log(e);
            res.writeHead(500, {'Content-Type': 'application/octet-stream'});
            res.end();
            return;
        }
        res.writeHead(200, {'Content-Type': 'application/octet-stream', 'Content-Length': outBuf.length});
        res.end(outBuf);
    }
}

module.exports.StateFun = StateFun;
module.exports.FunctionSpec = FunctionSpec;
module.exports.ValueSpec = ValueSpec;
module.exports.Address = Address;
module.exports.Message = Message;
module.exports.Context = Context;
module.exports.message_builder = message_builder;
module.exports.egress_message_builder = egress_message_builder;
module.exports.kafka_egress_message = kafka_egress_message;
module.exports.kinesis_egress_message = kinesis_egress_message;