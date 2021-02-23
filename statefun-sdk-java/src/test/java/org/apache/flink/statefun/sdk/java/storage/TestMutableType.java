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

package org.apache.flink.statefun.sdk.java.storage;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;

public class TestMutableType implements Type<TestMutableType.Type> {

  @Override
  public TypeName typeName() {
    return TypeName.typeNameOf("test", "my-mutable-type");
  }

  @Override
  public TypeSerializer<TestMutableType.Type> typeSerializer() {
    return new Serializer();
  }

  public static class Type {
    private String value;

    public Type(String value) {
      this.value = value;
    }

    public void mutate(String newValue) {
      this.value = newValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Type type = (Type) o;
      return Objects.equals(value, type.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }

  private static class Serializer implements TypeSerializer<TestMutableType.Type> {
    @Override
    public Slice serialize(Type value) {
      return Slices.wrap(value.value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Type deserialize(Slice bytes) {
      return new Type(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
    }
  }
}
