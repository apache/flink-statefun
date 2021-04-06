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
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufTypeInformation;
import org.apache.flink.statefun.sdk.state.PersistedValue;

/**
 * DynamicallyRegisteredTypes are types that are types that were discovered during runtime, for
 * example registered {@linkplain PersistedValue}s.
 */
@NotThreadSafe
public final class DynamicallyRegisteredTypes {

  private final StaticallyRegisteredTypes staticallyKnownTypes;
  private final Map<Class<?>, TypeInformation<?>> registeredTypes = new HashMap<>();

  public DynamicallyRegisteredTypes(StaticallyRegisteredTypes staticallyKnownTypes) {
    this.staticallyKnownTypes = Objects.requireNonNull(staticallyKnownTypes);
  }

  @SuppressWarnings("unchecked")
  public <T> TypeInformation<T> registerType(Class<T> type) {
    TypeInformation<T> typeInfo = staticallyKnownTypes.getType(type);
    if (typeInfo != null) {
      return typeInfo;
    }
    return (TypeInformation<T>) registeredTypes.computeIfAbsent(type, this::typeInformation);
  }

  @SuppressWarnings("unchecked")
  private TypeInformation<?> typeInformation(Class<?> valueType) {
    if (Message.class.isAssignableFrom(valueType)) {
      Class<Message> message = (Class<Message>) valueType;
      return new ProtobufTypeInformation<>(message);
    }
    // TODO: we may want to restrict the allowed typeInfo here to theses that respect schema
    // evaluation.
    return TypeInformation.of(valueType);
  }
}
