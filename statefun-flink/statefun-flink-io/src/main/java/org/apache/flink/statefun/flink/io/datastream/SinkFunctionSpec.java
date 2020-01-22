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
package org.apache.flink.statefun.flink.io.datastream;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * An {@link EgressSpec} that can run any Apache Flink {@link SinkFunction}.
 *
 * @param <T> The input type output by the sink.
 */
public final class SinkFunctionSpec<T> implements EgressSpec<T>, Serializable {
  private static final long serialVersionUID = 1;

  static final EgressType TYPE =
      new EgressType("org.apache.flink.statefun.flink.io", "sink-function-spec");

  private final EgressIdentifier<T> id;
  private final SinkFunction<T> delegate;

  /**
   * @param id A unique egress identifier.
   * @param delegate The underlying sink that the egress will delegate to at runtime.
   */
  public SinkFunctionSpec(EgressIdentifier<T> id, SinkFunction<T> delegate) {
    this.id = Objects.requireNonNull(id);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public final EgressIdentifier<T> id() {
    return id;
  }

  @Override
  public final EgressType type() {
    return TYPE;
  }

  SinkFunction<T> delegate() {
    return delegate;
  }
}
