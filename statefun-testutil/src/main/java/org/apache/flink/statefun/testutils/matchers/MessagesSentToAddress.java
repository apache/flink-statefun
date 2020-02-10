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

import java.util.List;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** A matcher for checking all the responses sent to a particular function type. */
public class MessagesSentToAddress extends TypeSafeMatcher<Map<Address, List<Object>>> {

  private final Map<Address, List<Matcher<?>>> matcherByAddress;

  MessagesSentToAddress(Map<Address, List<Matcher<?>>> matcherByAddress) {
    this.matcherByAddress = matcherByAddress;
  }

  @Override
  protected boolean matchesSafely(Map<Address, List<Object>> item) {
    for (Map.Entry<Address, List<Matcher<?>>> entry : matcherByAddress.entrySet()) {
      List<Object> messages = item.get(entry.getKey());
      if (messages == null) {
        return false;
      }

      if (messages.size() != entry.getValue().size()) {
        return false;
      }

      for (int i = 0; i < messages.size(); i++) {
        Matcher<?> matcher = entry.getValue().get(i);
        if (!matcher.matches(messages.get(i))) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("<{");

    for (Map.Entry<Address, List<Matcher<?>>> entry : matcherByAddress.entrySet()) {
      description
          .appendText(entry.getKey().toString())
          .appendText("=")
          .appendList("[", ",", "]", entry.getValue());
    }

    description.appendText("}>");
  }
}
