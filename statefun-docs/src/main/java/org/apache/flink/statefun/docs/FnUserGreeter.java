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
package org.apache.flink.statefun.docs;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class FnUserGreeter implements StatefulFunction {

  public static FunctionType TYPE = new FunctionType("example", "greeter");

  @Persisted
  private final PersistedValue<Integer> count = PersistedValue.of("count", Integer.class);

  public void invoke(Context context, Object input) {
    String userId = context.self().id();
    int seen = count.getOrDefault(0);

    switch (seen) {
      case 0:
        System.out.println(String.format("Hello %s!", userId));
        break;
      case 1:
        System.out.println("Hello Again!");
        break;
      case 2:
        System.out.println("Third time is the charm :)");
        break;
      default:
        System.out.println(String.format("Hello for the %d-th time", seen + 1));
    }

    count.set(seen + 1);
  }
}
