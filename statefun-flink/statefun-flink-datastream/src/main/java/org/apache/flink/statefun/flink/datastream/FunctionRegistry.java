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

package org.apache.flink.statefun.flink.datastream;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;

class FunctionRegistry {
  private final Map<FunctionType, SerializableStatefulFunctionProvider> specificTypeProviders =
      new HashMap<>();
  private final Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs =
      new HashMap<>();
  private final Map<FunctionTypeNamespaceMatcher, HttpFunctionEndpointSpec>
      perNamespaceEndpointSpecs = new HashMap<>();

  void put(FunctionType functionType, SerializableStatefulFunctionProvider provider) {
    putAndThrowIfPresent(specificTypeProviders, functionType, provider);
  }

  void add(HttpFunctionEndpointSpec spec) {
    if (spec.target().isSpecificFunctionType()) {
      putAndThrowIfPresent(specificTypeEndpointSpecs, spec.target().asSpecificFunctionType(), spec);
    } else {
      putAndThrowIfPresent(perNamespaceEndpointSpecs, spec.target().asNamespace(), spec);
    }
  }

  Functions getFunctions() {
    Map<String, HttpFunctionEndpointSpec> namespaceSpecs = new HashMap<>();
    perNamespaceEndpointSpecs.forEach(
        (namespace, spec) -> namespaceSpecs.put(namespace.targetNamespace(), spec));

    SerializableHttpFunctionProvider httpFunctionProvider =
        new SerializableHttpFunctionProvider(specificTypeEndpointSpecs, namespaceSpecs);
    specificTypeEndpointSpecs.forEach(
        (type, unused) -> specificTypeProviders.put(type, httpFunctionProvider));

    Map<FunctionTypeNamespaceMatcher, SerializableStatefulFunctionProvider> namespaceTypeProviders =
        new HashMap<>();
    perNamespaceEndpointSpecs.forEach(
        (type, unused) -> namespaceTypeProviders.put(type, httpFunctionProvider));

    return new Functions(specificTypeProviders, namespaceTypeProviders);
  }

  private static <K, V> void putAndThrowIfPresent(Map<K, V> map, K key, V value) {
    @Nullable V previous = map.put(key, value);
    if (previous == null) {
      return;
    }
    throw new IllegalStateException(
        String.format("A binding for the key %s was previously defined.", key));
  }

  static class Functions {
    private final Map<FunctionType, SerializableStatefulFunctionProvider> specificFunctions;
    private final Map<FunctionTypeNamespaceMatcher, SerializableStatefulFunctionProvider>
        namespaceFunctions;

    private Functions(
        Map<FunctionType, SerializableStatefulFunctionProvider> specificFunctions,
        Map<FunctionTypeNamespaceMatcher, SerializableStatefulFunctionProvider>
            namespaceFunctions) {
      this.specificFunctions = specificFunctions;
      this.namespaceFunctions = namespaceFunctions;
    }

    public Map<FunctionType, SerializableStatefulFunctionProvider> getSpecificFunctions() {
      return specificFunctions;
    }

    public Map<FunctionTypeNamespaceMatcher, SerializableStatefulFunctionProvider>
        getNamespaceFunctions() {
      return namespaceFunctions;
    }
  }
}
