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

package org.apache.flink.statefun.sdk.state;

import org.apache.flink.statefun.sdk.TypeName;
import org.junit.Test;

public class PersistedStateRegistryTest {

  @Test
  public void exampleUsage() {
    final PersistedStateRegistry registry = new PersistedStateRegistry();

    registry.registerValue(PersistedValue.of("value", String.class));
    registry.registerTable(PersistedTable.of("table", String.class, Integer.class));
    registry.registerAppendingBuffer(PersistedAppendingBuffer.of("buffer", String.class));
    registry.registerRemoteValue(
        RemotePersistedValue.of("remote", TypeName.parseFrom("io.statefun.types/raw")));
  }

  @Test(expected = IllegalStateException.class)
  public void duplicateRegistration() {
    final PersistedStateRegistry registry = new PersistedStateRegistry();

    registry.registerValue(PersistedValue.of("my-state", String.class));
    registry.registerTable(PersistedTable.of("my-state", String.class, Integer.class));
  }
}
