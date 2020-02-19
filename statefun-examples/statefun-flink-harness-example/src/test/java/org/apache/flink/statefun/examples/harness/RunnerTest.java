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
package org.apache.flink.statefun.examples.harness;

import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;
import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;
import org.apache.flink.util.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

public class RunnerTest {

  @Ignore(
      "This has an infinite egress and it would never complete, un-ignore to execute in the IDE")
  @Test
  public void run() throws Exception {
    Harness harness =
        new Harness()
            .withKryoMessageSerializer()
            .withSupplyingIngress(MyConstants.REQUEST_INGRESS, new MessageGenerator())
            .withPrintingEgress(MyConstants.RESULT_EGRESS);

    harness.start();
  }

  /** generate a random message, once a second a second. */
  private static final class MessageGenerator
      implements SerializableSupplier<MyMessages.MyInputMessage> {

    private static final long serialVersionUID = 1;

    @Override
    public MyMessages.MyInputMessage get() {
      try {
        Thread.sleep(1_000);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted", e);
      }
      return randomMessage();
    }

    @Nonnull
    private MyMessages.MyInputMessage randomMessage() {
      final ThreadLocalRandom random = ThreadLocalRandom.current();
      final String userId = StringUtils.generateRandomAlphanumericString(random, 2);
      return new MyMessages.MyInputMessage(userId, "hello " + userId);
    }
  }
}
