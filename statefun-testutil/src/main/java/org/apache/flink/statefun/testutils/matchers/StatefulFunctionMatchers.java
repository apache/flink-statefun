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
package org.apache.flink.statefun.testutils.matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.hamcrest.Matcher;

/**
 * A set of Hamcrest matchers to help check the responses from a {@link
 * org.apache.flink.statefun.testutils.function.FunctionTestHarness}
 *
 * <p>{@see FunctionTestHarness} for usage details.
 */
public final class StatefulFunctionMatchers {

  private StatefulFunctionMatchers() {
    throw new AssertionError();
  }

  public static MatchersByAddress messagesTo(
      Address to, Matcher<?> matcher, Matcher<?>... matchers) {
    List<Matcher<?>> allMatchers = new ArrayList<>(1 + matchers.length);
    allMatchers.add(matcher);
    allMatchers.addAll(Arrays.asList(matchers));

    return new MatchersByAddress(to, allMatchers);
  }

  /**
   * A matcher that checks all the responses sent to a given {@link FunctionType}.
   *
   * <p><b>Important:</b> This matcher expects an exact match on the number of responses sent to
   * this function.
   */
  public static MessagesSentToAddress sent(
      MatchersByAddress matcher, MatchersByAddress... matchers) {
    Map<Address, List<Matcher<?>>> messagesByAddress = new HashMap<>();
    messagesByAddress.put(matcher.address, matcher.matchers);

    for (MatchersByAddress match : matchers) {
      messagesByAddress.put(match.address, match.matchers);
    }

    return new MessagesSentToAddress(messagesByAddress);
  }

  /** A matcher that checks the function did not send any messages. */
  public static SentNothingMatcher sentNothing() {
    return new SentNothingMatcher();
  }
}
