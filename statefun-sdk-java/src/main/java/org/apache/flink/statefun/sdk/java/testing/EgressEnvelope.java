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
package org.apache.flink.statefun.sdk.java.testing;

import java.util.Objects;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;

/**
 * A utility class that wraps a {@link EgressMessage} thats was sent by a {@link
 * org.apache.flink.statefun.sdk.java.StatefulFunction}. It is used by the {@link TestContext}.
 */
public class EgressEnvelope {

  private final EgressMessage message;

  public EgressEnvelope(EgressMessage message) {
    this.message = Objects.requireNonNull(message);
  }

  public EgressMessage getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EgressEnvelope envelope = (EgressEnvelope) o;
    return Objects.equals(message, envelope.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EgressEnvelope{");
    sb.append("message=").append(message);
    sb.append('}');
    return sb.toString();
  }
}
