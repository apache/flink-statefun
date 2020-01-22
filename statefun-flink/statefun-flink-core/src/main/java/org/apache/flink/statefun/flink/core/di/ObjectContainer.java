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
package org.apache.flink.statefun.flink.core.di;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;

/** Minimal dependency injection. */
@Internal
public class ObjectContainer {

  private final Map<Key, Supplier<Object>> factories = new HashMap<>();

  private final Map<Key, Object> instances = new HashMap<>();

  public <T> void add(T singleton) {
    Class<?> type = singleton.getClass();
    factories.put(new Key(type), () -> singleton);
  }

  public <T> void add(Class<T> type) {
    factories.put(new Key(type), () -> createReflectively(type));
  }

  public <T> void add(String label, Class<? super T> type, T singleton) {
    factories.put(new Key(type, label), () -> singleton);
  }

  public <T> void add(String label, Class<? super T> type, Class<?> actual) {
    factories.put(new Key(type, label), () -> createReflectively(actual));
  }

  public <T> void add(String label, Lazy<T> lazyValue) {
    factories.put(new Key(Lazy.class, label), () -> lazyValue.withContainer(this));
  }

  public <T> T get(Class<T> type) {
    return get(type, null);
  }

  public <T> T get(Class<T> type, String label) {
    Key key = new Key(type, label);
    return getOrCreateInstance(key);
  }

  @SuppressWarnings("unchecked")
  private <T> T getOrCreateInstance(Key key) {
    @Nullable Object instance = instances.get(key);
    if (instance == null) {
      instances.put(key, instance = create(key));
    }
    return (T) instance;
  }

  private Object create(Key key) {
    Supplier<Object> factory = factories.get(key);
    if (factory == null) {
      throw new IllegalArgumentException("was not able to find a factory for " + key);
    }
    return factory.get();
  }

  private Object createReflectively(Class<?> type) {
    Constructor<?> constructor = findConstructorForInjection(type);
    Class<?>[] dependencies = constructor.getParameterTypes();
    Annotation[][] annotations = constructor.getParameterAnnotations();
    Object[] resolvedDependencies = new Object[dependencies.length];
    int i = 0;
    for (Class<?> dependency : dependencies) {
      @Nullable String label = findLabel(annotations[i]);
      Key key = new Key(dependency, label);
      resolvedDependencies[i] = getOrCreateInstance(key);
      i++;
    }
    try {
      constructor.setAccessible(true);
      return constructor.newInstance(resolvedDependencies);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  private static String findLabel(Annotation[] annotations) {
    for (Annotation annotation : annotations) {
      if (annotation.annotationType() == Label.class) {
        return ((Label) annotation).value();
      }
    }
    return null;
  }

  private static Constructor<?> findConstructorForInjection(Class<?> type) {
    Constructor<?>[] constructors = type.getDeclaredConstructors();
    Constructor<?> defaultCont = null;
    for (Constructor<?> constructor : constructors) {
      Annotation annotation = constructor.getAnnotation(Inject.class);
      if (annotation != null) {
        return constructor;
      }
      if (constructor.getParameterCount() == 0) {
        defaultCont = constructor;
      }
    }
    if (defaultCont != null) {
      return defaultCont;
    }
    throw new RuntimeException("not injectable type " + type);
  }

  private static final class Key {
    final Class<?> type;

    @Nullable final String label;

    Key(Class<?> type, @Nullable String label) {
      this.type = type;
      this.label = label;
    }

    Key(Class<?> type) {
      this(type, null);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return Objects.equals(type, key.type) && Objects.equals(label, key.label);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, label);
    }

    @Override
    public String toString() {
      return "Key{" + "type=" + type + ", label='" + label + '\'' + '}';
    }
  }
}
