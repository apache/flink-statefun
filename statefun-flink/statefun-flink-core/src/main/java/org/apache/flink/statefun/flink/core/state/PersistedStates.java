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
import org.apache.flink.statefun.sdk.state.*;

public final class PersistedStates {

  public static void findReflectivelyAndBind(
      @Nullable Object instance, FlinkStateBinder stateBinder) {
    List<?> states = findReflectively(instance);
    for (Object persisted : states) {
      if (persisted instanceof PersistedStateRegistry) {
        PersistedStateRegistry stateRegistry = (PersistedStateRegistry) persisted;
        ApiExtension.bindPersistedStateRegistry(stateRegistry, stateBinder);
      } else {
        stateBinder.bind(persisted);
      }
    }
  }

  private static List<?> findReflectively(@Nullable Object instance) {
    PersistedStates visitor = new PersistedStates();
    visitor.visit(instance);
    return visitor.getPersistedStates();
  }

  private final List<Object> persistedStates = new ArrayList<>();

  private void visit(@Nullable Object instance) {
    if (instance == null) {
      return;
    }
    for (Field field : findAnnotatedFields(instance.getClass(), Persisted.class)) {
      visitField(instance, field);
    }
  }

  private List<Object> getPersistedStates() {
    return persistedStates;
  }

  private void visitField(@Nonnull Object instance, @Nonnull Field field) {
    if (Modifier.isStatic(field.getModifiers())) {
      throw new IllegalArgumentException(
          "Static persisted states are not legal in: "
              + field.getType()
              + " on "
              + instance.getClass().getName());
    }
    Object persistedState = getPersistedStateReflectively(instance, field);
    if (persistedState == null) {
      throw new IllegalStateException(
          "The field " + field + " of a " + instance.getClass().getName() + " was not initialized");
    }
    Class<?> fieldType = field.getType();
    if (isPersistedState(fieldType) || isPersistedState(fieldType.getGenericSuperclass().getClass())) {
      persistedStates.add(persistedState);
    } else {
      List<?> innerFields = findReflectively(persistedState);
      persistedStates.addAll(innerFields);
    }
  }

  private static boolean isPersistedState(Class<?> fieldType) {
      return fieldType == PersistedValue.class
              || fieldType == PersistedAsyncValue.class
          || fieldType == PersistedIntegerValue.class
          || fieldType == PersistedAsyncIntegerValue.class
          || fieldType == PersistedTable.class
          || fieldType == PersistedAppendingBuffer.class
          || fieldType == PersistedList.class;
  }

  private static Object getPersistedStateReflectively(Object instance, Field persistedField) {
    try {
      persistedField.setAccessible(true);
      return persistedField.get(instance);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Unable access field " + persistedField.getName() + " of " + instance.getClass());
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
