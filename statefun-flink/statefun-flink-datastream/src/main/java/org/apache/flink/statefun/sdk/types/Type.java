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
package org.apache.flink.statefun.sdk.types;

import java.io.Serializable;
import org.apache.flink.statefun.sdk.TypeName;

/**
 * This class is the core abstraction used by Stateful Functions's type system, and consists of a
 * few things that StateFun uses to handle {@code Message}s and {@code ValueSpec}s:
 *
 * <ul>
 *   <li>A {@link TypeName} to identify the type.
 *   <li>A {@link TypeSerializer} for serializing and deserializing instances of the type.
 * </ul>
 *
 * <h2>Cross-language primitive types</h2>
 *
 * <p>StateFun's type system has cross-language support for common primitive types, such as boolean,
 * integer, long, etc. These primitive types have built-in {@link Type}s implemented for them
 * already, with predefined {@link TypeName}s.
 *
 * <p>This is of course all transparent for the user, so you don't need to worry about it. Functions
 * implemented in various languages (e.g. Java or Python) can message each other by directly sending
 * supported primitive values as message arguments.
 *
 * <h2>Common custom types (e.g. JSON or Protobuf)</h2>
 *
 * <p>The type system is also very easily extensible to support custom message types, such as JSON
 * or Protobuf messages. This is just a matter of implementing your own {@link Type} with a custom
 * typename and serializer. Alternatively, you can also use the {@link SimpleType} class to do this
 * easily.
 *
 * @param <T> the Java type of serialized / deserialized instances.
 */
public interface Type<T> extends Serializable {

  /** @return The unique {@link TypeName} of this type. */
  TypeName typeName();

  /** @return A {@link TypeSerializer} that can serialize and deserialize this type. */
  TypeSerializer<T> typeSerializer();
}
