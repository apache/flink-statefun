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

package org.apache.flink.statefun.sdk.types;

import java.io.IOException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.sdk.TypeName;

@Internal
public class TypeNameSerializer extends TypeSerializerSingleton<TypeName> {

  public static final TypeNameSerializer INSTANCE = new TypeNameSerializer();

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TypeName createInstance() {
    return new TypeName("", "");
  }

  @Override
  public TypeName copy(TypeName from) {
    return from;
  }

  @Override
  public TypeName copy(TypeName from, TypeName reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(TypeName record, DataOutputView target) throws IOException {
    StringSerializer.INSTANCE.serialize(record.canonicalTypenameString(), target);
  }

  @Override
  public TypeName deserialize(DataInputView source) throws IOException {
    return TypeName.parseFrom(StringSerializer.INSTANCE.deserialize(source));
  }

  @Override
  public TypeName deserialize(TypeName reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    StringSerializer.INSTANCE.copy(source, target);
  }

  @Override
  public TypeSerializerSnapshot<TypeName> snapshotConfiguration() {
    return new TypeNameSerializerSnapshot();
  }

  /** Serializer configuration snapshot for compatibility and format evolution. */
  @SuppressWarnings("WeakerAccess")
  public static final class TypeNameSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<TypeName> {

    public TypeNameSerializerSnapshot() {
      super(() -> INSTANCE);
    }
  }
}
