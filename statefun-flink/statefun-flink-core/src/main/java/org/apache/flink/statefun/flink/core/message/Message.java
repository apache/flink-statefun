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

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.flink.core.memory.DataOutputView;

public interface Message extends RoutableMessage {

  Object payload(MessageFactory context, ClassLoader targetClassLoader);

  /**
   * isBarrierMessage - returns an empty optional for non barrier messages or wrapped checkpointId
   * for barrier messages.
   *
   * <p>When this message represents a checkpoint barrier, this method returns an {@code Optional}
   * of a checkpoint id that produced that barrier. For other types of messages (i.e. {@code
   * Payload}) this method returns an empty {@code Optional}.
   */
  OptionalLong isBarrierMessage();

  Optional<String> cancellationToken();

  Message copy(MessageFactory context);

  void writeTo(MessageFactory context, DataOutputView target) throws IOException;

  default void postApply() {}
}
