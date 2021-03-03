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

package org.apache.flink.statefun.sdk.java.types;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.junit.Test;

public class SimpleTypeTest {

  @Test
  public void mutableType() {
    final Type<String> type =
        SimpleType.simpleTypeFrom(
            TypeName.typeNameFromString("test/simple-mutable-type"), String::getBytes, String::new);

    assertThat(type.typeName(), is(TypeName.typeNameFromString("test/simple-mutable-type")));
    assertRoundTrip(type, "hello world!");
  }

  @Test
  public void immutableType() {
    final Type<String> type =
        SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("test/simple-immutable-type"),
            String::getBytes,
            String::new);

    assertThat(type.typeName(), is(TypeName.typeNameFromString("test/simple-immutable-type")));
    assertRoundTrip(type, "hello world!");
  }

  public <T> void assertRoundTrip(Type<T> type, T element) {
    final Slice slice;
    {
      TypeSerializer<T> serializer = type.typeSerializer();
      slice = serializer.serialize(element);
    }
    TypeSerializer<T> serializer = type.typeSerializer();
    T deserialized = serializer.deserialize(slice);
    assertEquals(element, deserialized);
  }
}
