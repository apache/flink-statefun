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
package org.apache.flink.statefun.flink.core.pool;

import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Simple element pool.
 *
 * @param <ElementT> type of elements being pooled.
 */
@NotThreadSafe
public final class SimplePool<IType, ElementT> {
  private final ArrayDeque<ElementT> elements = new ArrayDeque<>();
  private final Function<IType, ElementT> supplier;
  private final int maxCapacity;

  public SimplePool(Function<IType, ElementT> supplier, int maxCapacity) {
    this.supplier = Objects.requireNonNull(supplier);
    this.maxCapacity = maxCapacity;
  }

  public ElementT get(IType controller) {
    ElementT element = elements.pollFirst();
    if (element != null) {
      return element;
    }
    return supplier.apply(controller);
  }

  public void release(ElementT item) {
    if (elements.size() < maxCapacity) {
      elements.addFirst(item);
    }
  }
}
