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

package org.apache.flink.statefun.flink.core.types.remote;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.sdk.TypeName;

public class RemoteValueSerializerSnapshot implements TypeSerializerSnapshot<byte[]> {
  private static final Integer VERSION = 1;

  private TypeName type;

  // empty constructor for restore paths
  public RemoteValueSerializerSnapshot() {}

  RemoteValueSerializerSnapshot(TypeName type) {
    this.type = type;
  }

  public TypeName type() {
    return type;
  }

  @Override
  public int getCurrentVersion() {
    return VERSION;
  }

  @Override
  public void writeSnapshot(DataOutputView dataOutputView) throws IOException {
    dataOutputView.writeUTF(type.namespace());
    dataOutputView.writeUTF(type.name());
  }

  @Override
  public void readSnapshot(int i, DataInputView dataInputView, ClassLoader classLoader)
      throws IOException {
    final String namespace = dataInputView.readUTF();
    final String name = dataInputView.readUTF();
    this.type = new TypeName(namespace, name);
  }

  @Override
  public TypeSerializer<byte[]> restoreSerializer() {
    return new RemoteValueSerializer(type);
  }

  @Override
  public TypeSerializerSchemaCompatibility<byte[]> resolveSchemaCompatibility(
      TypeSerializer<byte[]> otherSerializer) {
    if (!(otherSerializer instanceof RemoteValueSerializer)) {
      return TypeSerializerSchemaCompatibility.incompatible();
    }

    final RemoteValueSerializer otherRemoteTypeSerializer = (RemoteValueSerializer) otherSerializer;
    if (!type.equals(otherRemoteTypeSerializer.getType())) {
      // throw an exception to bubble up information about the previous snapshotted typename
      // TODO would this mess with Flink's schema compatibility checks?
      // TODO this should be fine, since at the moment, if we return incompatible, Flink immediately
      // fails anyways
      throw new RemoteValueTypeMismatchException(type, otherRemoteTypeSerializer.getType());
    }
    return TypeSerializerSchemaCompatibility.compatibleAsIs();
  }
}
