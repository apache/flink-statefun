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
package org.apache.flink.statefun.sdk.java;

import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Objects;

/**
 * This class represents the type of a {@code StatefulFunction}, consisting of a namespace of the
 * function type as well as the type's name.
 *
 * <p>A function's type is part of a function's {@link Address} and serves as integral part of an
 * individual function's identity.
 *
 * @see Address
 */
public final class TypeName implements Serializable {

  private static final long serialVersionUID = 1;

  private final String namespace;
  private final String type;
  private final String typenameString;
  private final ByteString typenameByteString;

  public static TypeName typeNameOf(String namespace, String name) {
    Objects.requireNonNull(namespace);
    Objects.requireNonNull(name);
    if (namespace.endsWith("/")) {
      namespace = namespace.substring(0, namespace.length() - 1);
    }
    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("namespace can not be empty.");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException("name can not be empty.");
    }
    return new TypeName(namespace, name);
  }

  public static TypeName typeNameFromString(String typeNameString) {
    Objects.requireNonNull(typeNameString);
    final int pos = typeNameString.lastIndexOf("/");
    if (pos <= 0 || pos == typeNameString.length() - 1) {
      throw new IllegalArgumentException(
          typeNameString + " does not conform to the <namespace>/<name> format");
    }
    String namespace = typeNameString.substring(0, pos);
    String name = typeNameString.substring(pos + 1);
    return typeNameOf(namespace, name);
  }

  /**
   * Creates a {@link TypeName}.
   *
   * @param namespace the function type's namepsace.
   * @param type the function type's name.
   */
  private TypeName(String namespace, String type) {
    this.namespace = Objects.requireNonNull(namespace);
    this.type = Objects.requireNonNull(type);
    String typenameString = canonicalTypeNameString(namespace, type);
    this.typenameString = typenameString;
    this.typenameByteString = ByteString.copyFromUtf8(typenameString);
  }

  /**
   * Returns the namespace of the function type.
   *
   * @return the namespace of the function type.
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the name of the function type.
   *
   * @return the name of the function type.
   */
  public String name() {
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
    TypeName functionType = (TypeName) o;
    return namespace.equals(functionType.namespace) && type.equals(functionType.type);
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
    return "TypeName(" + namespace + ", " + type + ")";
  }

  public String asTypeNameString() {
    return typenameString;
  }

  ByteString typeNameByteString() {
    return typenameByteString;
  }

  private static String canonicalTypeNameString(String namespace, String type) {
    return namespace + '/' + type;
  }
}
