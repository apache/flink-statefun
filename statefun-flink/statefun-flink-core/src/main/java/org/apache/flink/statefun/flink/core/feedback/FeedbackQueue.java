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
package org.apache.flink.statefun.flink.core.feedback;

import java.util.Deque;

/**
 * HandOffQueue - is a single producer single consumer (spsc) queue that supports adding and
 * draining atomically.
 *
 * <p>Implementors of this queue supports atomic addition operation (via {@link
 * #addAndCheckIfWasEmpty(Object)} and atomic, bulk retrieving of the content of this queue (via
 * {@link #drainAll()})}.
 *
 * @param <ElementT> element type that is stored in this queue.
 */
interface FeedbackQueue<ElementT> {

  /**
   * Adds an element to the queue atomically.
   *
   * @param element the element to add to the queue.
   * @return true, if prior to this addition the queue was empty.
   */
  boolean addAndCheckIfWasEmpty(ElementT element);

  /**
   * Atomically grabs all that elements of this queue.
   *
   * @return the elements present at the queue at the moment of this operation.
   */
  Deque<ElementT> drainAll();
}
