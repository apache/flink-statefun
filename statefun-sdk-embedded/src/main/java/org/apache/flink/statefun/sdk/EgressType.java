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
package org.apache.flink.statefun.sdk;

import java.util.Objects;
import org.apache.flink.statefun.sdk.io.EgressSpec;

/**
 * Defines the type of an egress, represented by a namespace and the type's name.
 *
 * <p>This is used by the system to translate an {@link EgressSpec} to a physical runtime-specific
 * representation.
 */
public final class EgressType {
  private final String namespace;
  private final String type;

  /**
   * Creates an {@link EgressType}.
   *
   * @param namespace the type's namespace.
   * @param type the type's name.
   */
  public EgressType(String namespace, String type) {
    this.namespace = Objects.requireNonNull(namespace);
    this.type = Objects.requireNonNull(type);
  }

  /**
   * Returns the namespace of this egress type.
   *
   * @return the namespace of this egress type.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of this egress type.
   *
   * @return the name of this egress type.
   */
  public String type() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EgressType that = (EgressType) o;
    return namespace.equals(that.namespace) && type.equals(that.type);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + namespace.hashCode();
    hash = 37 * hash + type.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return String.format("IngressType(%s, %s)", namespace, type);
  }
}
