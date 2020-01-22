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
package org.apache.flink.statefun.sdk.io;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class identifies an egress within a Stateful Functions application, and is part of an {@link
 * EgressSpec}.
 */
public final class EgressIdentifier<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String namespace;
  private final String name;
  private final Class<T> consumedType;

  /**
   * Creates an {@link EgressIdentifier}.
   *
   * @param namespace the namespace of the egress.
   * @param name the name of the egress.
   * @param consumedType the type of messages consumed by the egress.
   */
  public EgressIdentifier(String namespace, String name, Class<T> consumedType) {
    this.namespace = Objects.requireNonNull(namespace);
    this.name = Objects.requireNonNull(name);
    this.consumedType = Objects.requireNonNull(consumedType);
  }

  /**
   * Returns the namespace of the egress.
   *
   * @return the namespace of the egress.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of the egress.
   *
   * @return the name of the egress.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of messages consumed by the egress.
   *
   * @return the type of messages consumed by the egress.
   */
  public Class<T> consumedType() {
    return consumedType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EgressIdentifier<?> egressIdentifier = (EgressIdentifier<?>) o;
    return namespace.equals(egressIdentifier.namespace)
        && name.equals(egressIdentifier.name)
        && consumedType.equals(egressIdentifier.consumedType);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + namespace.hashCode();
    hash = 37 * hash + name.hashCode();
    hash = 37 * hash + consumedType.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return String.format("EgressKey(%s, %s, %s)", namespace, name, consumedType);
  }
}
