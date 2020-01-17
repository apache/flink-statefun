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
import com.ververica.statefun.state.processor.StateBootstrapFunction;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

/**
 * Represents a single state bootstrap data entry, tagged with the address of its target {@link
 * StateBootstrapFunction} as well as the index of its payload serializer within a union of multiple
 * {@link BootstrapDataset}s.
 *
 * @see BootstrapDatasetUnion#apply(List)
 */
@SuppressWarnings("WeakerAccess")
public class TaggedBootstrapData {

  private static final Address DEFAULT_ADDRESS =
      new Address(new FunctionType("ververica", "DEFAULT"), "DEFAULT");
  private static final Object DEFAULT_PAYLOAD = "DEFAULT_PAYLOAD";
  private static final int DEFAULT_UNION_INDEX = 0;

  private Address target;
  private Object payload;

  /**
   * Index of the payload serializer within the union serializer. See {@link
   * TaggedBootstrapDataSerializer}.
   */
  private int unionIndex;

  public static TaggedBootstrapData createDefaultInstance() {
    return new TaggedBootstrapData(DEFAULT_ADDRESS, DEFAULT_PAYLOAD, DEFAULT_UNION_INDEX);
  }

  public TaggedBootstrapData(Address target, Object payload, int unionIndex) {
    this.target = Objects.requireNonNull(target);
    this.payload = Objects.requireNonNull(payload);

    Preconditions.checkArgument(unionIndex >= 0);
    this.unionIndex = unionIndex;
  }

  public Address getTarget() {
    return target;
  }

  public void setTarget(Address target) {
    this.target = target;
  }

  public Object getPayload() {
    return payload;
  }

  public void setPayload(Object payload) {
    this.payload = payload;
  }

  public int getUnionIndex() {
    return unionIndex;
  }

  public void setUnionIndex(int unionIndex) {
    this.unionIndex = unionIndex;
  }

  public TaggedBootstrapData copy(TypeSerializer<Object> payloadSerializer) {
    return new TaggedBootstrapData(
        new Address(target.type(), target.id()), payloadSerializer.copy(payload), unionIndex);
  }
}
