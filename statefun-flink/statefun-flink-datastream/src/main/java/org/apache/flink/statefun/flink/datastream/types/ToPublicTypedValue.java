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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.TypedValue;

/**
 * Converts internal {@link org.apache.flink.statefun.sdk.reqreply.generated.TypedValue} to public
 * {@link TypedValue}.
 */
@Internal
public class ToPublicTypedValue
    implements MapFunction<
        org.apache.flink.statefun.sdk.reqreply.generated.TypedValue, TypedValue> {

  @Override
  public TypedValue map(org.apache.flink.statefun.sdk.reqreply.generated.TypedValue value)
      throws Exception {
    return new TypedValue(TypeName.parseFrom(value.getTypename()), value.toByteArray());
  }
}