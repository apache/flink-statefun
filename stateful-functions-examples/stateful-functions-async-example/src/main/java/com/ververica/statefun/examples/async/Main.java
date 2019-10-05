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

package com.ververica.statefun.examples.async;

import com.ververica.statefun.examples.async.events.TaskCompletionEvent;
import com.ververica.statefun.examples.async.events.TaskStartedEvent;
import com.ververica.statefun.flink.harness.Harness;
import com.ververica.statefun.flink.harness.io.SerializableConsumer;
import com.ververica.statefun.flink.harness.io.SerializableSupplier;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;
import org.apache.flink.util.StringUtils;

public class Main {

  public static void main(String[] args) throws Exception {
    Harness harness =
        new Harness()
            .noCheckpointing()
            .withKryoMessageSerializer()
            .withSupplyingIngress(Constants.REQUEST_INGRESS, new MessageGenerator())
            .withConsumingEgress(Constants.RESULT_EGRESS, new MessagePrinter());

    harness.start();
  }

  /** generate a random message, once a second a second. */
  private static final class MessageGenerator implements SerializableSupplier<TaskStartedEvent> {

    private static final long serialVersionUID = 1;

    @Override
    public TaskStartedEvent get() {
      try {
        Thread.sleep(1_000);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted", e);
      }
      return randomMessage();
    }

    @Nonnull
    private TaskStartedEvent randomMessage() {
      final ThreadLocalRandom random = ThreadLocalRandom.current();
      final String taskId = StringUtils.generateRandomAlphanumericString(random, 2);
      return new TaskStartedEvent(taskId, System.currentTimeMillis());
    }
  }

  /** prints the messages to stdout. */
  private static final class MessagePrinter implements SerializableConsumer<TaskCompletionEvent> {

    private static final long serialVersionUID = 1;

    @Override
    public void accept(TaskCompletionEvent message) {
      System.out.println(
          String.format(
              "Task %s has completed, total duration: %d ms",
              message.getTaskId(), message.getEndTime() - message.getStartTime()));
    }
  }
}
