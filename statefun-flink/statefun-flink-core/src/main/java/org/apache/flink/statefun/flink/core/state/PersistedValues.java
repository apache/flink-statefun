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
package org.apache.flink.statefun.flink.core.state;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class PersistedValues {

  static List<PersistedValue<Object>> findReflectively(@Nullable Object instance) {
    PersistedValues visitor = new PersistedValues();
    visitor.visit(instance);
    return visitor.getPersistedValues();
  }

  private final List<PersistedValue<Object>> persistedValues = new ArrayList<>();

  private void visit(@Nullable Object instance) {
    if (instance == null) {
      return;
    }
    for (Field field : findAnnotatedFields(instance.getClass(), Persisted.class)) {
      visitField(instance, field);
    }
  }

  private List<PersistedValue<Object>> getPersistedValues() {
    return persistedValues;
  }

  private void visitField(@Nonnull Object instance, @Nonnull Field field) {
    if (field.getType() != PersistedValue.class) {
      throw new IllegalArgumentException(
          "Unknown persisted value type "
              + field.getType()
              + " on "
              + instance.getClass().getName());
    }
    if (Modifier.isStatic(field.getModifiers())) {
      throw new IllegalArgumentException(
          "Static persisted values are not legal in: "
              + field.getType()
              + " on "
              + instance.getClass().getName());
    }
    PersistedValue<Object> persistedValue = getPersistedValueReflectively(instance, field);
    if (persistedValue == null) {
      throw new IllegalStateException(
          "The field " + field + " of a " + instance.getClass().getName() + " was not initialized");
    }
    persistedValues.add(persistedValue);
  }

  @SuppressWarnings("unchecked")
  private static PersistedValue<Object> getPersistedValueReflectively(
      Object instance, Field persistedValueField) {
    try {
      persistedValueField.setAccessible(true);
      return (PersistedValue<Object>) persistedValueField.get(instance);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Unable access field " + persistedValueField.getName() + " of " + instance.getClass());
    }
  }

  public static Iterable<Field> findAnnotatedFields(
      Class<?> javaClass, Class<? extends Annotation> annotation) {
    Stream<Field> fields =
        definedFields(javaClass).filter(field -> field.getAnnotation(annotation) != null);

    return fields::iterator;
  }

  private static Stream<Field> definedFields(Class<?> javaClass) {
    if (javaClass == null || javaClass == Object.class) {
      return Stream.empty();
    }
    Stream<Field> selfMethods = Arrays.stream(javaClass.getDeclaredFields());
    Stream<Field> superMethods = definedFields(javaClass.getSuperclass());
    return Stream.concat(selfMethods, superMethods);
  }
}
