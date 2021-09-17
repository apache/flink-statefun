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
import {StateFun} from '../src/statefun';

function assertThrows(ex: any, fn: () => any) {
    let failed = false;
    try {
        fn();
    } catch (e) {
        failed = true;
    }
    ex(failed).toStrictEqual(true);
}


describe('StateFun', () => {
    it('Should demonstrate a usage of binding a function', () => {
        let sf = new StateFun();

        sf.bind({
            typename: "com.foo.fns/greeter",
            specs: [
                {
                    name: "seen",
                    type: StateFun.intType()
                }
            ],

            fn(context, message) {
            }
        });
    });

    it('Should demonstrate a usage of binding a function with no states', () => {
        let sf = new StateFun();

        sf.bind({
            typename: "com.foo.fns/greeter",
            fn(context, message) {
            }
        });
    });


    it('Should fail with a bad typename', () => {
        assertThrows(expect, () => {

            let sf = new StateFun();
            sf.bind({
                typename: "/greeter",
                fn(context, message) {
                }
            });


        });
    });

    it('Should fail with a bad spec name', () => {
        assertThrows(expect, () => {

            let sf = new StateFun();
            sf.bind({
                typename: "foo/greeter",
                specs: [{name: "a b", type: StateFun.intType()}],
                fn(context, message) {
                }
            });

        });
    });

    it('Should fail with duplicate spec names', () => {
        assertThrows(expect, () => {

            let sf = new StateFun();
            sf.bind({
                typename: "foo/greeter",
                specs: [
                    {name: "a", type: StateFun.intType()},
                    {name: "a", type: StateFun.intType()}
                ],
                fn(context, message) {
                }
            });

        });
    });

});