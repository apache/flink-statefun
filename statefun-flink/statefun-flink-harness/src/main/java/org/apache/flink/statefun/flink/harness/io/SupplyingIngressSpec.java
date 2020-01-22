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
package org.apache.flink.statefun.flink.harness.io;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

public final class SupplyingIngressSpec<T> implements IngressSpec<T>, Serializable {

  private static final long serialVersionUID = 1;

  private final IngressIdentifier<T> id;
  private final SerializableSupplier<T> supplier;
  private final long delayInMilliseconds;

  public SupplyingIngressSpec(
      IngressIdentifier<T> id,
      SerializableSupplier<T> supplier,
      long productionDelayInMilliseconds) {
    this.id = Objects.requireNonNull(id);
    this.supplier = Objects.requireNonNull(supplier);
    this.delayInMilliseconds = productionDelayInMilliseconds;
  }

  @Override
  public IngressIdentifier<T> id() {
    return id;
  }

  @Override
  public IngressType type() {
    return HarnessConstants.SUPPLYING_INGRESS_TYPE;
  }

  SerializableSupplier<T> supplier() {
    return supplier;
  }

  long delayInMilliseconds() {
    return delayInMilliseconds;
  }
}
