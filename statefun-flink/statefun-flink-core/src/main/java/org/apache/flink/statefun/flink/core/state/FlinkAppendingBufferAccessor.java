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
package org.apache.flink.statefun.flink.core.state;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.statefun.sdk.state.AppendingBufferAccessor;

final class FlinkAppendingBufferAccessor<E> implements AppendingBufferAccessor<E> {

  private final ListState<E> handle;

  FlinkAppendingBufferAccessor(ListState<E> handle) {
    this.handle = Objects.requireNonNull(handle);
  }

  @Override
  public void append(E element) {
    try {
      this.handle.add(element);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void appendAll(List<E> elements) {
    try {
      this.handle.addAll(elements);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void replaceWith(List<E> elements) {
    try {
      this.handle.update(elements);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public Iterable<E> view() {
    try {
      return this.handle.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      this.handle.clear();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
