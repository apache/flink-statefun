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
package org.apache.flink.statefun.sdk.java;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.junit.Test;

public class StatefulFunctionSpecTest {

  private static final ValueSpec<Integer> STATE_A = ValueSpec.named("state_a").withIntType();
  private static final ValueSpec<Boolean> STATE_B = ValueSpec.named("state_b").withBooleanType();

  @Test
  public void exampleUsage() {
    final StatefulFunctionSpec spec =
        StatefulFunctionSpec.builder(TypeName.typeNameOf("test.namespace", "test.name"))
            .withValueSpecs(STATE_A, STATE_B)
            .withSupplier(TestFunction::new)
            .build();

    assertThat(spec.supplier().get(), instanceOf(TestFunction.class));
    assertThat(spec.typeName(), is(TypeName.typeNameOf("test.namespace", "test.name")));
    assertThat(spec.knownValues(), hasKey("state_a"));
    assertThat(spec.knownValues(), hasKey("state_b"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateRegistration() {
    StatefulFunctionSpec.builder(TypeName.typeNameOf("test.namespace", "test.name"))
        .withValueSpecs(
            ValueSpec.named("foobar").withIntType(), ValueSpec.named("foobar").withBooleanType());
  }

  private static class TestFunction implements StatefulFunction {
    @Override
    public CompletableFuture<Void> apply(Context context, Message argument) {
      // no-op
      return context.done();
    }
  }
}
