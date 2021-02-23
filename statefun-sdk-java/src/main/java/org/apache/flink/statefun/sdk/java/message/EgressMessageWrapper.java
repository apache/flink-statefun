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
package org.apache.flink.statefun.sdk.java.message;

import java.util.Objects;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

@Internal
public final class EgressMessageWrapper implements EgressMessage {
  private final TypedValue typedValue;
  private final TypeName targetEgressId;

  public EgressMessageWrapper(TypeName targetEgressId, TypedValue actualMessage) {
    this.targetEgressId = Objects.requireNonNull(targetEgressId);
    this.typedValue = Objects.requireNonNull(actualMessage);
  }

  @Override
  public TypeName targetEgressId() {
    return targetEgressId;
  }

  @Override
  public TypeName egressMessageValueType() {
    return TypeName.typeNameFromString(typedValue.getTypename());
  }

  @Override
  public Slice egressMessageValueBytes() {
    return SliceProtobufUtil.asSlice(typedValue.getValue());
  }

  public TypedValue typedValue() {
    return typedValue;
  }
}
