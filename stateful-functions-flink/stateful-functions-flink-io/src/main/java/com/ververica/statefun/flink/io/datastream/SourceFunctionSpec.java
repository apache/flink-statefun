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

package com.ververica.statefun.flink.io.datastream;

import com.ververica.statefun.sdk.IngressType;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * An {@link IngressSpec} that can run any Apache Flink {@link SourceFunction}.
 *
 * @param <T> The input type consumed by the source.
 */
public final class SourceFunctionSpec<T> implements IngressSpec<T>, Serializable {
  private static final long serialVersionUID = 1;

  static final IngressType TYPE =
      new IngressType("com.ververica.statefun.flink.io", "source-function-spec");

  private final IngressIdentifier<T> id;
  private final SourceFunction<T> delegate;

  /**
   * @param id A unique ingress identifier.
   * @param delegate The underlying source function that this spec will delegate to at runtime.
   */
  public SourceFunctionSpec(IngressIdentifier<T> id, SourceFunction<T> delegate) {
    this.id = Objects.requireNonNull(id);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public final IngressIdentifier<T> id() {
    return id;
  }

  @Override
  public final IngressType type() {
    return TYPE;
  }

  SourceFunction<T> delegate() {
    return delegate;
  }
}
