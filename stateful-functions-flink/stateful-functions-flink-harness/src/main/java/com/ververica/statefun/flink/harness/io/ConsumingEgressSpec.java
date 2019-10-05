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

package com.ververica.statefun.flink.harness.io;

import com.ververica.statefun.sdk.EgressType;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.EgressSpec;
import java.io.Serializable;
import java.util.Objects;

public final class ConsumingEgressSpec<T> implements EgressSpec<T>, Serializable {

  private static final long serialVersionUID = 1;

  private final EgressIdentifier<T> id;
  private final SerializableConsumer<T> consumer;

  public ConsumingEgressSpec(EgressIdentifier<T> id, SerializableConsumer<T> consumer) {
    this.id = Objects.requireNonNull(id);
    this.consumer = Objects.requireNonNull(consumer);
  }

  @Override
  public EgressIdentifier<T> id() {
    return id;
  }

  @Override
  public EgressType type() {
    return HarnessConstants.CONSUMING_EGRESS_TYPE;
  }

  SerializableConsumer<T> consumer() {
    return consumer;
  }
}
