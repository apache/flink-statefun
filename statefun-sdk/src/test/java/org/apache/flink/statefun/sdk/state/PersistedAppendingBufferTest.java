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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Test;

public class PersistedAppendingBufferTest {

  @Test
  public void viewOnInit() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    assertFalse(buffer.view().iterator().hasNext());
  }

  @Test
  public void append() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");

    assertThat(buffer.view().iterator().next(), is("element"));
  }

  @Test
  public void appendAll() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.appendAll(Arrays.asList("element-1", "element-2"));

    assertThat(buffer.view(), contains("element-1", "element-2"));
  }

  @Test
  public void appendAllEmptyList() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.appendAll(new ArrayList<>());

    assertThat(buffer.view().iterator().next(), is("element"));
  }

  @Test
  public void replaceWith() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.replaceWith(Collections.singletonList("element-new"));

    assertThat(buffer.view().iterator().next(), is("element-new"));
  }

  @Test
  public void replaceWithEmptyList() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.replaceWith(new ArrayList<>());

    assertFalse(buffer.view().iterator().hasNext());
  }

  @Test
  public void viewAfterClear() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");
    buffer.clear();

    assertFalse(buffer.view().iterator().hasNext());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void viewUnmodifiable() {
    PersistedAppendingBuffer<String> buffer = PersistedAppendingBuffer.of("test", String.class);
    buffer.append("element");

    Iterator<String> view = buffer.view().iterator();
    view.remove();
  }
}
