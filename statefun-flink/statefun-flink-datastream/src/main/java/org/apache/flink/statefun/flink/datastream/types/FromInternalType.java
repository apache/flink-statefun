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

package org.apache.flink.statefun.flink.datastream.types;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.slice.Slice;
import org.apache.flink.statefun.sdk.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.types.Type;
import org.apache.flink.statefun.sdk.types.TypeSerializer;

@Internal
public class FromInternalType<T> extends RichMapFunction<TypedValue, T> {

  private final Type<T> type;

  public FromInternalType(Type<T> type) {
    this.type = type;
  }

  @Override
  public T map(TypedValue typedValue) throws Exception {
    TypeSerializer<T> typeSerializer = type.typeSerializer();
    Slice input = SliceProtobufUtil.asSlice(typedValue.getValue());
    return typeSerializer.deserialize(input);
  }
}
