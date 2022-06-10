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
package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.java.handler.MoreFutures.applySequentially;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.IntStream;
import org.junit.Test;

public class MoreFuturesTest {

  @Test
  public void usageExample() {
    final ConcurrentLinkedDeque<String> results = new ConcurrentLinkedDeque<>();

    CompletableFuture<?> allDone =
        applySequentially(
            Arrays.asList("a", "b", "c"),
            letter -> CompletableFuture.runAsync(() -> results.addLast(letter)));

    allDone.join();

    assertThat(results, contains("a", "b", "c"));
  }

  @Test
  public void firstThrows() {
    CompletableFuture<Void> allDone =
        applySequentially(Arrays.asList("a", "b", "c"), throwing("a"));

    assertThat(allDone.isCompletedExceptionally(), is(true));
  }

  @Test
  public void lastThrows() {
    CompletableFuture<Void> allDone =
        applySequentially(Arrays.asList("a", "b", "c"), throwing("c"));

    assertThat(allDone.isCompletedExceptionally(), is(true));
  }

  @Test
  public void bigList() {
    Iterator<String> lotsOfInts =
        IntStream.range(0, 1_000_000).mapToObj(String::valueOf).iterator();
    Iterable<String> input = () -> lotsOfInts;

    CompletableFuture<Void> allDone = applySequentially(input, throwing(null));

    allDone.join();
  }

  private static MoreFutures.Fn<String, CompletableFuture<Void>> throwing(String when) {
    return letter -> {
      if (letter.equals(when)) {
        throw new IllegalStateException("I'm a throwing function");
      } else {
        return CompletableFuture.completedFuture(null);
        //        return CompletableFuture.runAsync(
        //            () -> {
        ////              try {
        ////                Thread.sleep(1_00);
        ////              } catch (InterruptedException e) {
        ////                e.printStackTrace();
        ////              }
        //              /* complete successfully */
        //            });
      }
    };
  }
}
