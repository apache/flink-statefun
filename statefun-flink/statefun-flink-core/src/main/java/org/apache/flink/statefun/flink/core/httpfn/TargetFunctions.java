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

package org.apache.flink.statefun.flink.core.httpfn;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.TypeName;

public abstract class TargetFunctions implements Serializable {

  public static TargetFunctions fromPatternString(String patternString) {
    TypeName targetTypeName = TypeName.parseFrom(patternString);
    if (targetTypeName.namespace().contains("*")) {
      throw new IllegalArgumentException(
          "Invalid syntax for target functions. Only <namespace>/<name> or <namespace>/* are supported.");
    }
    if (targetTypeName.name().equals("*")) {
      return TargetFunctions.namespace(targetTypeName.namespace());
    }
    if (targetTypeName.name().contains("*")) {
      throw new IllegalArgumentException(
          "Invalid syntax for target functions. Only <namespace>/<name> or <namespace>/* are supported.");
    }
    final FunctionType functionType =
        new FunctionType(targetTypeName.namespace(), targetTypeName.name());
    return TargetFunctions.functionType(functionType);
  }

  public static TargetFunctions namespace(String namespace) {
    return new TargetFunctions.NamespaceTarget(
        FunctionTypeNamespaceMatcher.targetNamespace(namespace));
  }

  public static TargetFunctions functionType(FunctionType functionType) {
    return new TargetFunctions.FunctionTypeTarget(functionType);
  }

  public boolean isSpecificFunctionType() {
    return this.getClass() == TargetFunctions.FunctionTypeTarget.class;
  }

  public boolean isNamespace() {
    return this.getClass() == TargetFunctions.NamespaceTarget.class;
  }

  public abstract FunctionTypeNamespaceMatcher asNamespace();

  public abstract FunctionType asSpecificFunctionType();

  private static class NamespaceTarget extends TargetFunctions {
    private static final long serialVersionUID = 1;

    private final FunctionTypeNamespaceMatcher namespaceMatcher;

    private NamespaceTarget(FunctionTypeNamespaceMatcher namespaceMatcher) {
      this.namespaceMatcher = Objects.requireNonNull(namespaceMatcher);
    }

    @Override
    public FunctionTypeNamespaceMatcher asNamespace() {
      return namespaceMatcher;
    }

    @Override
    public FunctionType asSpecificFunctionType() {
      throw new IllegalStateException("This target is not a specific function type");
    }
  }

  private static class FunctionTypeTarget extends TargetFunctions {
    private static final long serialVersionUID = 1;

    private final FunctionType functionType;

    private FunctionTypeTarget(FunctionType functionType) {
      this.functionType = Objects.requireNonNull(functionType);
    }

    @Override
    public FunctionTypeNamespaceMatcher asNamespace() {
      throw new IllegalStateException("This target is not a namespace.");
    }

    @Override
    public FunctionType asSpecificFunctionType() {
      return functionType;
    }
  }
}
