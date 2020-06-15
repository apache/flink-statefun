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

import org.apache.flink.statefun.sdk.FunctionType;

public abstract class StateBinder {
  public abstract void bindValue(PersistedValue<?> persistedValue, FunctionType functionType);

  public abstract void bindTable(PersistedTable<?, ?> persistedTable, FunctionType functionType);

  public abstract void bindAppendingBuffer(
      PersistedAppendingBuffer<?> persistedAppendingBuffer, FunctionType functionType);

  public final void bind(Object stateObject, FunctionType functionType) {
    if (stateObject instanceof PersistedValue) {
      bindValue((PersistedValue<?>) stateObject, functionType);
    } else if (stateObject instanceof PersistedTable) {
      bindTable((PersistedTable<?, ?>) stateObject, functionType);
    } else if (stateObject instanceof PersistedAppendingBuffer) {
      bindAppendingBuffer((PersistedAppendingBuffer<?>) stateObject, functionType);
    } else {
      throw new IllegalArgumentException("Unknown persisted state object " + stateObject);
    }
  }
}
