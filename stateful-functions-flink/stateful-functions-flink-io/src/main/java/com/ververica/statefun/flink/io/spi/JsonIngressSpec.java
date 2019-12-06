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
package com.ververica.statefun.flink.io.spi;

import com.ververica.statefun.sdk.IngressType;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public final class JsonIngressSpec<T> implements IngressSpec<T> {
  private final JsonNode json;
  private final IngressIdentifier<T> id;
  private final IngressType type;

  public JsonIngressSpec(IngressType type, IngressIdentifier<T> id, JsonNode json) {
    this.type = Objects.requireNonNull(type);
    this.id = Objects.requireNonNull(id);
    this.json = Objects.requireNonNull(json);
  }

  @Override
  public IngressType type() {
    return type;
  }

  @Override
  public IngressIdentifier<T> id() {
    return id;
  }

  public JsonNode json() {
    return json;
  }
}
