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
package org.apache.flink.statefun.sdk.io;

import org.apache.flink.statefun.sdk.EgressType;

/**
 * Complete specification for an egress, containing of the egress' {@link EgressIdentifier} and the
 * {@link EgressType}. This fully defines an egress within a Stateful Functions application.
 *
 * <p>This serves as a "logical" representation of an output sink that stateful functions within an
 * application can send messages to. Under the scenes, the system translates this to a physical
 * runtime-specific representation corresponding to the specified {@link EgressType}.
 *
 * @param <T> the type of messages consumed by this egress.
 */
public interface EgressSpec<T> {

  /**
   * Returns the unique identifier of the egress.
   *
   * @return the unique identifier of the egress.
   */
  EgressIdentifier<T> id();

  /**
   * Returns the type of the egress.
   *
   * @return the type of the egress.
   */
  EgressType type();
}
