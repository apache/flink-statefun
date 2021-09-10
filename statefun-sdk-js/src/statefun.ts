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
    readonly #fns: Record<string, FunctionSpec>;

    constructor() {
        this.#fns = {};
    }

    /**
     * Bind a single function.
     *
     * Here is a minimal example:
     *
     * bind({
     *     typename: "foo.bar/baz",
     *     fn(context, message) {
     *         console.log(message.asString());
     *     }
     * });
     *
     * To define states, use the specs keys:
     *
     * bind({
     *      ...
     *      specs: [
     *          {
     *              name: seen,
     *              type: StateFun.intType()
     *              expireAfterWrite: 1000 * 60 * 60 * 24
     *          }
     *      ],
     *
     *      fn(context, message) {
     *              context.storage.seen += 1;
     *              console.log(context.storage.seen);
     *      }
     * });
     *
     * each defined spec will appear as a property on the address scoped storage, and will be manged
     * automatically by the runtime.
     */
    bind(opts: FunctionOpts) {
        const spec = FunctionSpec.fromOpts(opts);
        this.#fns[spec.typename] = spec;
    }

    static intType(): Type<number> {
        return INT_TYPE;
    }

    static stringType(): Type<string> {
        return STRING_TYPE;
    }

    static booleanType(): Type<boolean> {
        return BOOL_TYPE;
    }

    static floatType(): Type<number> {
        return FLOAT_TYPE;
    }

    /**
     * Creates a Type that can marshal/unmarshal JSON objects.
     * @param {string} typename a string of the form <namespace>/<name> that represents this Type's name.
     */
    static jsonType<T>(typename: string): Type<T> {
        return new JsonType(typename);
    }

    /**
     * Creates a Type that can marshal/unmarshal Protobuf generated JavaScript classes.
     *
     * @param {string} typename typename a string of the form <namespace>/<name> that represents this Type's name.
     * @param {any} googleProtobufGeneratedType a JavaScript class that was generated using the protoc compiler.
     */
    static protoType<T>(typename: string, googleProtobufGeneratedType: any): Type<T> {
        return new ProtobufType(typename, googleProtobufGeneratedType);
    }

    /**
     * Creates a Type that use the provided two functions to serialize/deserialize the values with.
     *
     * @param typename typename a string of the form <namespace>/<name> that represents this Type's name.
     * @param serialize a function that will be used to serialize instance of this types to bytes.
     * @param deserializer a function that will be used to deserialize bytes to instances of this type.
     */
    static customType<T>(typename: string, serialize: (a: T) => Buffer, deserializer: (buf: Buffer) => T): Type<T> {
        return new CustomType(typename, serialize, deserializer);
    }

    /**
     * An HTTP request response handler function for NodeJs that dispatches requests from the StateFun cluster to bound functions.
     * A typical usage will be:
     *
     * statefun = new Statefun();
     * statefun.bind( ... );
     * statefun.bind( ... );
     * ..
     *
     * http.createServer(statefun.handler()).listen(8000);
     *
     * If a more granular control is needed, use the handle function directly.
     */
    handler() {
        const self = this;
        return async (req: any, res: any) => {
            await self.handle(req, res);
        }
    }

    async handle(req: any, res: any) {
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