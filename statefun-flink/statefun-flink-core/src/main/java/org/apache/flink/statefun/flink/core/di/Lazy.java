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

import java.util.Objects;
import javax.annotation.Nullable;

@SuppressWarnings({"unchecked", "unused", "WeakerAccess"})
public final class Lazy<T> {
  private final Class<T> type;
  private final String label;
  private ObjectContainer container;

  @Nullable private T instance;

  public Lazy(Class<T> type) {
    this(type, null);
  }

  public Lazy(Class<T> type, String label) {
    this.type = type;
    this.label = label;
  }

  public Lazy(T instance) {
    this((Class<T>) instance.getClass(), null);
    this.instance = instance;
  }

  Lazy<T> withContainer(ObjectContainer container) {
    this.container = Objects.requireNonNull(container);
    return this;
  }

  public T get() {
    @Nullable T instance = this.instance;
    if (instance == null) {
      instance = container.get(type, label);
      this.instance = instance;
    }
    return instance;
  }
}
