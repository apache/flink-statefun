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
package com.ververica.statefun.flink.core.jsonmodule;

import java.util.Objects;

final class NamespaceNamePair {
  public final String namespace;
  public final String name;

  public static NamespaceNamePair from(String namespaceAndName) {
    Objects.requireNonNull(namespaceAndName);
    final int pos = namespaceAndName.lastIndexOf("/");
    if (pos <= 0 || pos == namespaceAndName.length() - 1) {
      throw new IllegalArgumentException(
          namespaceAndName + " does not conform to the <namespace>/<name> format");
    }
    String namespace = namespaceAndName.substring(0, pos);
    String name = namespaceAndName.substring(pos + 1);
    return new NamespaceNamePair(namespace, name);
  }

  private NamespaceNamePair(String namespace, String name) {
    this.namespace = Objects.requireNonNull(namespace);
    this.name = Objects.requireNonNull(name);
  }
}
