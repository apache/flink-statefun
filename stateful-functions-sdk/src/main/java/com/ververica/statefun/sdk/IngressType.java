/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.sdk;

import com.ververica.statefun.sdk.io.IngressSpec;
import java.util.Objects;

/**
 * Defines the type of an ingress, represented by a namespace and the type's name.
 *
 * <p>This is used by the system to translate an {@link IngressSpec} to a physical runtime-specific
 * representation.
 */
public final class IngressType {
  private final String namespace;
  private final String type;

  /**
   * Creates an {@link IngressType}.
   *
   * @param namespace the type's namespace.
   * @param type the type's name.
   */
  public IngressType(String namespace, String type) {
    this.namespace = Objects.requireNonNull(namespace);
    this.type = Objects.requireNonNull(type);
  }

  /**
   * Returns the namespace of this ingress type.
   *
   * @return the namespace of this ingress type.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of this ingress type.
   *
   * @return the name of this ingress type.
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
    IngressType that = (IngressType) o;
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
