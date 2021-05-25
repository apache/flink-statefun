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
package org.apache.flink.statefun.sdk.java.testing;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.junit.Before;
import org.junit.Test;

public class TestContextTest {

  private TestContext context;
  private Address someone;
  private Address me;

  @Before
  public void resetContext() {
    me = new Address(TypeName.typeNameOf("com.example", "function"), "me");
    someone = new Address(TypeName.typeNameOf("com.example", "function"), "someone");
    context = new TestContext(me, someone);
  }

  @Test
  public void testSelfAndCaller() {
    assertThat(context.self(), equalTo(me));
    assertThat(context.caller(), equalTo(Optional.of(someone)));
  }

  @Test
  public void testRoundTripToAddressedScopeStorageWithBuiltInType() {
    ValueSpec<Boolean> builtInTypeSpec = ValueSpec.named("type").withBooleanType();
    context.storage().set(builtInTypeSpec, true);

    assertThat(context.storage().get(builtInTypeSpec), equalTo(Optional.of(true)));
  }

  @Test
  public void testRoundTripToAddressedScopeStorageWithCustomType() {

    Type<User> type =
        SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.example/User"), User::toBytes, User::new);

    User user = new User("someone".getBytes(StandardCharsets.UTF_8));

    ValueSpec<User> customTypeSpec = ValueSpec.named("type").withCustomType(type);
    context.storage().set(customTypeSpec, user);

    assertThat(context.storage().get(customTypeSpec), equalTo(Optional.of(user)));
  }

  private static class User {
    public String name;

    public User(byte[] bytes) {
      name = new String(bytes);
    }

    public byte[] toBytes() {
      return name.getBytes(StandardCharsets.UTF_8);
    }
  }
}
