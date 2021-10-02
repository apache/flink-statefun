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

import static org.apache.flink.statefun.sdk.utils.DataflowUtils.typeToFunctionTypeString;

/**
 * An {@link Address} is the unique identity of an individual {@link StatefulFunction}, containing
 * of the function's {@link FunctionType} and an unique identifier within the type. The function's
 * type denotes the class of function to invoke, while the unique identifier addresses the
 * invocation to a specific function instance.
 */
public final class Address {
  private final FunctionType type;
  private final String id;

  /**
   * Creates an {@link Address}.
   *
   * @param type type of the function.
   * @param id unique id within the function type.
   */
  public Address(FunctionType type, String id) {
    this.type = Objects.requireNonNull(type);
    this.id = Objects.requireNonNull(id);
  }

  /**
   * Returns the {@link FunctionType} that this address identifies.
   *
   * @return type of the function
   */
  public FunctionType type() {
    return type;
  }

  /**
   * Returns the unique function id, within its type, that this address identifies.
   *
   * @return unique id within the function type.
   */
  public String id() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Address address = (Address) o;
    return type.equals(address.type) && id.equals(address.id);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + type.hashCode();
    hash = 37 * hash + id.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return String.format("Address(%s, %s, %s) internalType %s raw %s", type.namespace(), type.name(), id,
            type.getInternalType()==null?"":typeToFunctionTypeString(type.getInternalType()), type.getInternalType()==null?"": type.getInternalType());
  }
}
