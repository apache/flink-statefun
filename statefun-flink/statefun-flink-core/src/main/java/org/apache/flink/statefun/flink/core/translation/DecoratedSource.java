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
package org.apache.flink.statefun.flink.core.translation;

import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

final class DecoratedSource {
  final String name;

  final String uid;

  final SourceFunction<?> source;

  private DecoratedSource(String name, String uid, SourceFunction<?> source) {
    this.name = name;
    this.uid = uid;
    this.source = source;
  }

  public static DecoratedSource of(IngressSpec<?> spec, SourceFunction<?> source) {
    IngressIdentifier<?> identifier = spec.id();
    String name = String.format("%s-%s-ingress", identifier.namespace(), identifier.name());
    String uid =
        String.format(
            "%s-%s-%s-%s-ingress",
            spec.type().namespace(), spec.type().type(), identifier.namespace(), identifier.name());

    return new DecoratedSource(name, uid, source);
  }
}
