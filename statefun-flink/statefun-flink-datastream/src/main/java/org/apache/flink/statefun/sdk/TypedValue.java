/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.sdk;

import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.statefun.sdk.types.TypedValueTypeInfoFactory;

/** A typed value containing a {@link TypeName} and serialized bytes. */
@TypeInfo(TypedValueTypeInfoFactory.class)
@SuppressWarnings("unused")
public class TypedValue {
  private TypeName typeName;

  private byte[] value;

  public TypedValue() {}

  public TypedValue(TypeName typeName, byte[] value) {
    this.typeName = Objects.requireNonNull(typeName);
    this.value = Objects.requireNonNull(value);
  }

  public void setTypeName(TypeName typeName) {
    this.typeName = typeName;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public TypeName getTypeName() {
    return this.typeName;
  }

  public byte[] getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TypedValue typedValue = (TypedValue) o;
    return Objects.equals(typeName, typedValue.typeName) && Arrays.equals(value, typedValue.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(typeName);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
