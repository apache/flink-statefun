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

package org.apache.flink.statefun.sdk.messages.serializer;

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.sdk.messages.EgressMessage;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.slice.Slice;
import org.apache.flink.statefun.sdk.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.types.Type;
import org.apache.flink.statefun.sdk.types.Types;

public class EgressMessageSerializerTest extends SerializerTestBase<EgressMessage> {

  @Override
  protected TypeSerializer<EgressMessage> createSerializer() {
    return EgressMessageSerializer.INSTANCE;
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<EgressMessage> getTypeClass() {
    return EgressMessage.class;
  }

  @Override
  protected EgressMessage[] getTestData() {
    return new EgressMessage[] {
      egressMessage(Types.integerType(), 1),
      egressMessage(Types.booleanType(), true),
      egressMessage(Types.longType(), 100L),
      egressMessage(Types.floatType(), 0.5f),
      egressMessage(Types.doubleType(), 12.2d),
      egressMessage(Types.stringType(), "hello")
    };
  }

  public static <T> EgressMessage egressMessage(Type<T> customType, T element) {
    Objects.requireNonNull(customType);
    Objects.requireNonNull(element);
    org.apache.flink.statefun.sdk.types.TypeSerializer<T> typeSerializer =
        customType.typeSerializer();
    TypedValue.Builder builder = TypedValue.newBuilder();
    builder.setTypename(customType.typeName().canonicalTypenameString());
    Slice serialized = typeSerializer.serialize(element);
    ByteString serializedByteString = SliceProtobufUtil.asByteString(serialized);
    builder.setValue(serializedByteString);
    builder.setHasValue(true);
    return new EgressMessage(builder.build());
  }
}
