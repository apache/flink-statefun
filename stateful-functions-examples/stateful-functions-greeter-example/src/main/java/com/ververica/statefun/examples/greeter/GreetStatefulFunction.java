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

package com.ververica.statefun.examples.greeter;

import com.ververica.statefun.examples.greeter.generated.GreetRequest;
import com.ververica.statefun.examples.greeter.generated.GreetResponse;
import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.StatefulFunction;
import com.ververica.statefun.sdk.annotations.Persisted;
import com.ververica.statefun.sdk.state.PersistedValue;

/**
 * A stateful function that generates a unique greeting for each user based on how many times that
 * user has been seen by the system.
 */
final class GreetStatefulFunction implements StatefulFunction {

  /**
   * The function type is the unique identifier that identifies this type of function. The type, in
   * conjunction with an identifier, is how routers and other functions can use to reference a
   * particular instance of a greeter function.
   *
   * <p>If this was a multi-module application, the function type could be in different package so
   * functions in other modules could message the greeter without a direct dependency on this class.
   */
  static final FunctionType TYPE = new FunctionType("ververica", "greeter");

  /**
   * The persisted value for maintaining state about a particular user. The value returned by this
   * field is always scoped to the current user. seenCount is the number of times the user has been
   * greeted.
   */
  @Persisted
  private final PersistedValue<Integer> seenCount = PersistedValue.of("seen-count", Integer.class);

  @Override
  public void invoke(Context context, Object input) {
    GreetRequest greetMessage = (GreetRequest) input;
    GreetResponse response = computePersonalizedGreeting(greetMessage);
    context.send(GreetingIO.GREETING_EGRESS_ID, response);
  }

  private GreetResponse computePersonalizedGreeting(GreetRequest greetMessage) {
    final String name = greetMessage.getWho();
    final int seen = seenCount.getOrDefault(0);
    seenCount.set(seen + 1);

    String greeting = greetText(name, seen);

    return GreetResponse.newBuilder().setWho(name).setGreeting(greeting).build();
  }

  private static String greetText(String name, int seen) {
    switch (seen) {
      case 0:
        return String.format("Hello %s ! \uD83D\uDE0E", name);
      case 1:
        return String.format("Hello again %s ! \uD83E\uDD17", name);
      case 2:
        return String.format("Third time is a charm! %s! \uD83E\uDD73", name);
      case 3:
        return String.format("Happy to see you once again %s ! \uD83D\uDE32", name);
      default:
        return String.format("Hello at the %d-th time %s \uD83D\uDE4C", seen + 1, name);
    }
  }
}
