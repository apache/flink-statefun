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
package org.apache.flink.statefun.sdk.java.handler;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

public class TestUtils {
  private TestUtils() {}

  public static <T> Matcher<FromFunction.PersistedValueMutation> modifiedValue(
      ValueSpec<T> spec, T newValue) {
    FromFunction.PersistedValueMutation mutation =
        FromFunction.PersistedValueMutation.newBuilder()
            .setStateName(spec.name())
            .setMutationType(FromFunction.PersistedValueMutation.MutationType.MODIFY)
            .setStateValue(typedValue(spec.type(), newValue))
            .build();

    return Matchers.is(mutation);
  }

  public static FromFunction.PersistedValueSpec protoFromValueSpec(ValueSpec<?> spec) {
    return ProtoUtils.protoFromValueSpec(spec).build();
  }

  public static <T> TypedValue.Builder typedValue(Type<T> type, T value) {
    Slice serializedValue = type.typeSerializer().serialize(value);
    return TypedValue.newBuilder()
        .setTypename(type.typeName().asTypeNameString())
        .setHasValue(value != null)
        .setValue(SliceProtobufUtil.asByteString(serializedValue));
  }

  /** A test utility to build ToFunction messages using SDK concepts. */
  public static final class RequestBuilder {

    private final ToFunction.InvocationBatchRequest.Builder builder =
        ToFunction.InvocationBatchRequest.newBuilder();

    public RequestBuilder withTarget(TypeName target, String id) {
      builder.setTarget(
          Address.newBuilder().setNamespace(target.namespace()).setType(target.name()).setId(id));
      return this;
    }

    public <T> RequestBuilder withState(ValueSpec<T> spec) {
      builder.addState(ToFunction.PersistedValue.newBuilder().setStateName(spec.name()));

      return this;
    }

    public <T> RequestBuilder withState(ValueSpec<T> spec, T value) {
      builder.addState(
          ToFunction.PersistedValue.newBuilder()
              .setStateName(spec.name())
              .setStateValue(typedValue(spec.type(), value)));

      return this;
    }

    public <T> RequestBuilder withInvocation(Type<T> type, T value) {
      builder.addInvocations(
          ToFunction.Invocation.newBuilder().setArgument(typedValue(type, value)));
      return this;
    }

    public ToFunction build() {
      return ToFunction.newBuilder().setInvocation(builder).build();
    }
  }
}
