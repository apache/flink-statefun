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

import {ValueSpec} from "../src/core";
import {StateFun} from "../src/statefun";
import {describe, expect} from '@jest/globals'

describe('ValueSpec', () => {
    it('Should be constructed correctly from a js object', () => {

        const spec = ValueSpec.fromOpts({name: "hello", type: StateFun.intType(), expireAfterCall: 123});

        expect(spec.name).toStrictEqual("hello");
        expect(spec.type).toStrictEqual(StateFun.intType());
        expect(spec.expireAfterCall).toStrictEqual(123);
        expect(spec.expireAfterWrite).toStrictEqual(-1);
    });

    it('Should be constructed correctly from a js object with expireAfterWrite', () => {
        const spec = ValueSpec.fromOpts({name: "hello", type: StateFun.intType(), expireAfterWrite: 123});

        expect(spec.name).toStrictEqual("hello");
        expect(spec.type).toStrictEqual(StateFun.intType());
        expect(spec.expireAfterCall).toStrictEqual(-1);
        expect(spec.expireAfterWrite).toStrictEqual(123);
    });

});
