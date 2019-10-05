/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.logger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.flink.core.memory.MemorySegment;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class MemorySegmentPoolTest {

  @Test
  public void emptyMemorySegmentPoolDoesNotAllocateSegments() {
    MemorySegmentPool pool = new MemorySegmentPool(0);

    assertThat(pool.nextSegment(), nullValue());
  }

  @Test
  public void emptyMemorySegmentPoolOverdraftsWhenAskedTo() {
    MemorySegmentPool pool = new MemorySegmentPool(0);

    pool.ensureAtLeastOneSegmentPresent();

    assertThat(pool.nextSegment(), notNullValue());
  }

  @Test
  public void emptyMemorySegmentPoolOverdraftsTemporally() {
    MemorySegmentPool pool = new MemorySegmentPool(0);

    pool.ensureAtLeastOneSegmentPresent();
    final MemorySegment overdraft = pool.nextSegment();
    pool.release(overdraft);

    assertThat(overdraft, notNullValue());
    assertThat(overdraft.isFreed(), is(true));
    assertThat(pool.nextSegment(), nullValue());
  }

  @Test
  public void minimalAllocationUnitIsPageSize() {
    MemorySegmentPool pool = new MemorySegmentPool(MemorySegmentPool.PAGE_SIZE - 1);

    assertThat(pool.nextSegment(), CoreMatchers.nullValue());
  }

  @Test
  public void poolIsAbleToAllocateTheRequiredNumberOfPages() {
    final int pageCount = 10;
    MemorySegmentPool pool = new MemorySegmentPool(pageCount * MemorySegmentPool.PAGE_SIZE);

    for (int i = 0; i < pageCount; i++) {
      MemorySegment segment = pool.nextSegment();

      assertThat(segment, notNullValue());
      assertThat(segment.size(), is(MemorySegmentPool.PAGE_SIZE));
    }

    assertThat(pool.nextSegment(), nullValue());
  }

  @SuppressWarnings("PointlessArithmeticExpression")
  @Test
  public void segmentsCanBeReturnedToThePool() {
    MemorySegmentPool pool = new MemorySegmentPool(1 * MemorySegmentPool.PAGE_SIZE);
    //
    // we can allocate at least 1 segment
    //
    MemorySegment segment = pool.nextSegment();
    assertThat(segment, notNullValue());
    //
    // we can allocate exactly 1 segment
    //
    assertThat(pool.nextSegment(), nullValue());
    //
    // return a segment to the pool
    //
    pool.release(segment);
    //
    // now we can use the segment
    //
    MemorySegment pooled = pool.nextSegment();
    assertThat(pooled, notNullValue());
    assertThat(pooled.isFreed(), is(false));
  }
}
