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

public final class FunctionTypeNamespaceMatcher implements Serializable {

  private static final long serialVersionUID = 1;

  private final String targetNamespace;

  public static FunctionTypeNamespaceMatcher targetNamespace(String namespace) {
    return new FunctionTypeNamespaceMatcher(namespace);
  }

  private FunctionTypeNamespaceMatcher(String targetNamespace) {
    this.targetNamespace = Objects.requireNonNull(targetNamespace);
  }

  public String targetNamespace() {
    return targetNamespace;
  }

  public boolean matches(FunctionType functionType) {
    return targetNamespace.equals(functionType.namespace());
  }

  @Override
  public int hashCode() {
    return targetNamespace.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FunctionTypeNamespaceMatcher other = (FunctionTypeNamespaceMatcher) obj;
    return targetNamespace.equals(other.targetNamespace);
  }

  @Override
  public String toString() {
    return String.format("FunctionTypeNamespaceMatcher(%s)", targetNamespace);
  }
}
