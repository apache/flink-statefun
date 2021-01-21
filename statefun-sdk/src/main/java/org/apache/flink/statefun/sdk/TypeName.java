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

import java.io.Serializable;
import java.util.Objects;

public final class TypeName implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String DELIMITER = "/";

  private final String namespace;
  private final String name;

  public static TypeName parseFrom(String typeNameString) {
    final String[] split = typeNameString.split(DELIMITER);
    if (split.length != 2) {
      throw new IllegalArgumentException(
          "Invalid type name string: "
              + typeNameString
              + ". Must be of format <namespace>"
              + DELIMITER
              + "<name>.");
    }
    return new TypeName(split[0], split[1]);
  }

  public TypeName(String namespace, String name) {
    this.namespace = Objects.requireNonNull(namespace);
    this.name = Objects.requireNonNull(name);
  }

  public String namespace() {
    return namespace;
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return namespace + DELIMITER + name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TypeName typeName = (TypeName) o;
    return Objects.equals(namespace, typeName.namespace) && Objects.equals(name, typeName.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name);
  }
}
