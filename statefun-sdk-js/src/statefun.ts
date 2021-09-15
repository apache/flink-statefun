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

import {FunctionSpec, ValueSpec, Address, Type, FunctionOpts} from "./core";
import {Context} from "./context";
import {Message, messageBuilder, egressMessageBuilder} from "./message";
import {kafkaEgressMessage, kinesisEgressMessage} from "./egress";
import {handle} from "./handler";

import {BOOL_TYPE, CustomType, FLOAT_TYPE, INT_TYPE, JsonType, ProtobufType, STRING_TYPE} from "./types";

class StateFun {
    readonly #fns;

    constructor() {
        this.#fns = {};
    }

    bind(opts: FunctionOpts) {
        const spec = FunctionSpec.fromOpts(opts);
        this.#fns[spec.typename] = spec;
    }

    static intType(): Type {
        return INT_TYPE;
    }

    static stringType(): Type {
        return STRING_TYPE;
    }

    static booleanType(): Type {
        return BOOL_TYPE;
    }

    static floatType(): Type {
        return FLOAT_TYPE;
    }

    static jsonType(typename): Type {
        return new JsonType(typename);
    }

    static protoType(typename, googleProtobufGeneratedType): Type {
        return new ProtobufType(typename, googleProtobufGeneratedType);
    }

    static customType(typename, serialize, deserializer): Type {
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

export {StateFun}
export {FunctionSpec}
export {ValueSpec}
export {Address}
export {Message}
export {Context}
export {messageBuilder}
export {egressMessageBuilder}
export {kafkaEgressMessage}
export {kinesisEgressMessage}