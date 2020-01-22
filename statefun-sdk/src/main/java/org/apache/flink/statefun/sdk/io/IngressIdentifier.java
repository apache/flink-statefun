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
 * This class identifies an ingress within a Stateful Functions application, and is part of an
 * {@link IngressSpec}.
 */
public final class IngressIdentifier<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String namespace;
  private final String name;
  private final Class<T> producedType;

  /**
   * Creates an {@link IngressIdentifier}.
   *
   * @param producedType the type of messages produced by the ingress.
   * @param namespace the namespace of the ingress.
   * @param name the name of the ingress.
   */
  public IngressIdentifier(Class<T> producedType, String namespace, String name) {
    this.namespace = Objects.requireNonNull(namespace);
    this.name = Objects.requireNonNull(name);
    this.producedType = Objects.requireNonNull(producedType);
  }

  /**
   * Returns the namespace of the ingress.
   *
   * @return the namespace of the ingress.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of the ingress.
   *
   * @return the name of the ingress.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of messages produced by the ingress.
   *
   * @return the type of messages produced by the ingress.
   */
  public Class<T> producedType() {
    return producedType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IngressIdentifier<?> that = (IngressIdentifier<?>) o;
    return namespace.equals(that.namespace)
        && name.equals(that.name)
        && producedType.equals(that.producedType);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + namespace.hashCode();
    hash = 37 * hash + name.hashCode();
    hash = 37 * hash + producedType.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return String.format("IngressIdentifier(%s, %s, %s)", namespace, name, producedType);
  }
}
