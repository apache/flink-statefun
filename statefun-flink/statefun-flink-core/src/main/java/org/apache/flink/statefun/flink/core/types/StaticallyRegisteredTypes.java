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
package org.apache.flink.statefun.flink.core.types;

import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufTypeInformation;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.MessageTypeInformation;

/**
 * StaticallyRegisteredTypes are types that were registered during the creation of the Stateful
 * Functions universe.
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public final class StaticallyRegisteredTypes {

  private final Map<Class<?>, TypeInformation<?>> registeredTypes = new HashMap<>();

  public StaticallyRegisteredTypes(MessageFactoryType messageFactoryType) {
    this.messageFactoryType = messageFactoryType;
  }

  private final MessageFactoryType messageFactoryType;

  public <T> TypeInformation<T> registerType(Class<T> type) {
    return (TypeInformation<T>) registeredTypes.computeIfAbsent(type, this::typeInformation);
  }

  /**
   * Retrieves the previously registered type. This is safe to access concurrently, after the
   * translation phase is over.
   */
  @Nullable
  <T> TypeInformation<T> getType(Class<T> valueType) {
    return (TypeInformation<T>) registeredTypes.get(valueType);
  }

  private TypeInformation<?> typeInformation(Class<?> valueType) {
    if (Message.class.isAssignableFrom(valueType)) {
      Class<Message> message = (Class<Message>) valueType;
      return new ProtobufTypeInformation<>(message);
    }
    if (org.apache.flink.statefun.flink.core.message.Message.class.isAssignableFrom(valueType)) {
      return new MessageTypeInformation(messageFactoryType);
    }
    // TODO: we may want to restrict the allowed typeInfo here to theses that respect shcema
    // evaluation.
    return TypeInformation.of(valueType);
  }
}
