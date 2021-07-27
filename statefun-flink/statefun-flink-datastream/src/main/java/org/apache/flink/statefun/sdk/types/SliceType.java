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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.slice.Slice;

public final class SliceType implements Type<Slice> {

  private static final Set<TypeCharacteristics> IMMUTABLE_TYPE_CHARS =
      Collections.unmodifiableSet(EnumSet.of(TypeCharacteristics.IMMUTABLE_VALUES));

  private final TypeName typename;
  private final Serializer serializer = new Serializer();

  public SliceType(TypeName typename) {
    this.typename = Objects.requireNonNull(typename);
  }

  @Override
  public TypeName typeName() {
    return typename;
  }

  @Override
  public TypeSerializer<Slice> typeSerializer() {
    return serializer;
  }

  @Override
  public Set<TypeCharacteristics> typeCharacteristics() {
    return IMMUTABLE_TYPE_CHARS;
  }

  private static final class Serializer implements TypeSerializer<Slice> {
    @Override
    public Slice serialize(Slice slice) {
      return slice;
    }

    @Override
    public Slice deserialize(Slice slice) {
      return slice;
    }
  }
}
