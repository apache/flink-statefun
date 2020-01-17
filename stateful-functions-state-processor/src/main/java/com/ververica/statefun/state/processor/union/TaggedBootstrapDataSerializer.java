/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.state.processor.union;

import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

/** Serializer for {@link TaggedBootstrapData}. */
public final class TaggedBootstrapDataSerializer extends TypeSerializer<TaggedBootstrapData> {

  private static final long serialVersionUID = 1L;

  private final TypeSerializer<Object>[] payloadSerializers;

  private transient Object[] reusablePayloadObjects;

  TaggedBootstrapDataSerializer(List<TypeSerializer<?>> payloadSerializers) {
    Preconditions.checkNotNull(payloadSerializers);
    Preconditions.checkArgument(!payloadSerializers.isEmpty());
    this.payloadSerializers = toPayloadSerializerIndexArray(payloadSerializers);
    this.reusablePayloadObjects = createReusablePayloadObjectsIndexArray(this.payloadSerializers);
  }

  @Override
  public boolean isImmutableType() {
    for (TypeSerializer<?> serializer : payloadSerializers) {
      if (!serializer.isImmutableType()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public TypeSerializer<TaggedBootstrapData> duplicate() {
    List<TypeSerializer<?>> duplicates = new ArrayList<>(payloadSerializers.length);
    boolean stateful = false;
    for (TypeSerializer<?> serializer : payloadSerializers) {
      TypeSerializer<?> duplicate = serializer.duplicate();
      if (duplicate != serializer) {
        stateful = true;
      }
      duplicates.add(duplicate);
    }
    if (!stateful) {
      return this;
    }
    return new TaggedBootstrapDataSerializer(duplicates);
  }

  @Override
  public TaggedBootstrapData createInstance() {
    return TaggedBootstrapData.createDefaultInstance();
  }

  @Override
  public void serialize(TaggedBootstrapData bootstrapData, DataOutputView dataOutputView)
      throws IOException {
    final int unionIndex = bootstrapData.getUnionIndex();
    final Address address = bootstrapData.getTarget();

    dataOutputView.writeInt(unionIndex);
    dataOutputView.writeUTF(address.type().namespace());
    dataOutputView.writeUTF(address.type().name());
    dataOutputView.writeUTF(address.id());
    payloadSerializers[unionIndex].serialize(bootstrapData.getPayload(), dataOutputView);
  }

  @Override
  public TaggedBootstrapData deserialize(DataInputView dataInputView) throws IOException {
    final int unionIndex = dataInputView.readInt();
    final String targetFunctionTypeNamespace = dataInputView.readUTF();
    final String targetFunctionTypeName = dataInputView.readUTF();
    final String targetFunctionId = dataInputView.readUTF();
    final Object payload = payloadSerializers[unionIndex].deserialize(dataInputView);

    return new TaggedBootstrapData(
        new Address(
            new FunctionType(targetFunctionTypeNamespace, targetFunctionTypeName),
            targetFunctionId),
        payload,
        unionIndex);
  }

  @Override
  public TaggedBootstrapData deserialize(TaggedBootstrapData reuse, DataInputView dataInputView)
      throws IOException {
    final int unionIndex = dataInputView.readInt();

    final String targetFunctionTypeNamespace = dataInputView.readUTF();
    final String targetFunctionTypeName = dataInputView.readUTF();
    final String targetFunctionId = dataInputView.readUTF();

    reuse.setUnionIndex(unionIndex);
    reuse.setTarget(
        new Address(
            new FunctionType(targetFunctionTypeNamespace, targetFunctionTypeName),
            targetFunctionId));
    reuse.setPayload(
        payloadSerializers[unionIndex].deserialize(
            reusablePayloadObjects[unionIndex], dataInputView));

    return reuse;
  }

  @Override
  public TaggedBootstrapData copy(TaggedBootstrapData bootstrapData) {
    final TypeSerializer<Object> payloadSerializer =
        payloadSerializers[bootstrapData.getUnionIndex()];
    return bootstrapData.copy(payloadSerializer);
  }

  @Override
  public TaggedBootstrapData copy(TaggedBootstrapData bootstrapData, TaggedBootstrapData reuse) {
    final int unionIndex = bootstrapData.getUnionIndex();
    final TypeSerializer<Object> payloadSerializer = payloadSerializers[unionIndex];
    final Object reusedPayloadCopy =
        payloadSerializer.copy(bootstrapData.getPayload(), reusablePayloadObjects[unionIndex]);

    final Address address = bootstrapData.getTarget();
    reuse.setTarget(new Address(address.type(), address.id()));
    reuse.setPayload(reusedPayloadCopy);
    reuse.setUnionIndex(bootstrapData.getUnionIndex());

    return reuse;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    final int unionIndex = source.readInt();

    target.writeInt(unionIndex);
    // -- function type namespace
    target.writeUTF(source.readUTF());
    // -- function type name
    target.writeUTF(source.readUTF());
    // -- function id
    target.writeUTF(source.readUTF());
    payloadSerializers[unionIndex].copy(source, target);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaggedBootstrapDataSerializer that = (TaggedBootstrapDataSerializer) o;
    return Arrays.equals(payloadSerializers, that.payloadSerializers);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(payloadSerializers);
  }

  @Override
  public TypeSerializerSnapshot<TaggedBootstrapData> snapshotConfiguration() {
    throw new UnsupportedOperationException(
        "This serializer should not have been used for any persistent data.");
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.reusablePayloadObjects = createReusablePayloadObjectsIndexArray(payloadSerializers);
  }

  @SuppressWarnings("unchecked")
  private static TypeSerializer<Object>[] toPayloadSerializerIndexArray(
      List<TypeSerializer<?>> payloadSerializers) {
    return payloadSerializers.toArray(new TypeSerializer[0]);
  }

  private static Object[] createReusablePayloadObjectsIndexArray(
      TypeSerializer<?>[] payloadSerializers) {
    final Object[] result = new Object[payloadSerializers.length];

    for (int index = 0; index < payloadSerializers.length; index++) {
      result[index] = payloadSerializers[index].createInstance();
      index++;
    }

    return result;
  }
}
