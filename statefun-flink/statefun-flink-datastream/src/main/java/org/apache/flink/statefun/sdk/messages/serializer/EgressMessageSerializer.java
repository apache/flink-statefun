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

import com.google.protobuf.MoreByteStrings;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.sdk.messages.EgressMessage;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class EgressMessageSerializer extends TypeSerializerSingleton<EgressMessage> {

  public static final EgressMessageSerializer INSTANCE = new EgressMessageSerializer();

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public EgressMessage createInstance() {
    return new EgressMessage(TypedValue.newBuilder().build());
  }

  @Override
  public EgressMessage copy(EgressMessage from) {
    return from;
  }

  @Override
  public EgressMessage copy(EgressMessage from, EgressMessage reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(EgressMessage record, DataOutputView target) throws IOException {
    target.writeUTF(record.typedValue().getTypename());
    target.writeBoolean(record.typedValue().getHasValue());
    target.writeInt(record.typedValue().getValue().size());
    target.write(record.typedValue().getValue().toByteArray());
  }

  @Override
  public EgressMessage deserialize(DataInputView source) throws IOException {
    TypedValue.Builder builder = TypedValue.newBuilder();
    builder.setTypename(source.readUTF());
    builder.setHasValue(source.readBoolean());
    int len = source.readInt();
    byte[] data = new byte[len];
    source.read(data);
    builder.setValue(MoreByteStrings.wrap(data));
    return new EgressMessage(builder.build());
  }

  @Override
  public EgressMessage deserialize(EgressMessage reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeUTF(source.readUTF());
    target.writeBoolean(source.readBoolean());
    int len = source.readInt();
    target.writeInt(len);
    target.write(source, len);
  }

  @Override
  public TypeSerializerSnapshot<EgressMessage> snapshotConfiguration() {
    return new EgressMessageTypeSerializer();
  }

  public static class EgressMessageTypeSerializer
      extends SimpleTypeSerializerSnapshot<EgressMessage> {

    public EgressMessageTypeSerializer() {
      super(() -> INSTANCE);
    }
  }
}
