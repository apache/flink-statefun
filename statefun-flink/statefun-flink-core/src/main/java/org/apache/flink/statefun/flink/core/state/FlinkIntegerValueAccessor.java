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
package org.apache.flink.statefun.flink.core.state;


import org.apache.flink.api.common.state.IntegerValueState;
import org.apache.flink.api.common.state.IntegerValueStateDescriptor;
import org.apache.flink.statefun.sdk.state.IntegerAccessor;

final class FlinkIntegerValueAccessor extends FlinkValueAccessor<Long> implements IntegerAccessor {


    FlinkIntegerValueAccessor(IntegerValueState handle, IntegerValueStateDescriptor descriptor) {
        super(handle, descriptor);
    }



    public Long incr(){
        return ((IntegerValueState)handle).incr();
    }

    @Override
    public void clear() {
        handle.clear();
    }
}