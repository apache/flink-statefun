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
package org.apache.flink.statefun.flink.core.message;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public final class MessageFactoryKey implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageFactoryType type;
  private final String customPayloadSerializerClassName;

  private MessageFactoryKey(MessageFactoryType type, String customPayloadSerializerClassName) {
    this.type = Objects.requireNonNull(type);
    this.customPayloadSerializerClassName = customPayloadSerializerClassName;
  }

  public static MessageFactoryKey forType(
      MessageFactoryType type, String customPayloadSerializerClassName) {
    return new MessageFactoryKey(type, customPayloadSerializerClassName);
  }

  public MessageFactoryType getType() {
    return this.type;
  }

  public Optional<String> getCustomPayloadSerializerClassName() {
    return Optional.ofNullable(customPayloadSerializerClassName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MessageFactoryKey that = (MessageFactoryKey) o;
    return type == that.type
        && Objects.equals(customPayloadSerializerClassName, that.customPayloadSerializerClassName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, customPayloadSerializerClassName);
  }
}
