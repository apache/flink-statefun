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

package com.ververica.statefun.flink.core.common;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxSender;

@Internal
public final class StreamTaskMailbox {

  public static Executor mailboxExecutor(StreamTask<?, ?> containingTask) {
    MailboxSender sender = obtainContainingStreamTaskMailboxSenderForFlink19(containingTask);
    return new MailboxAdapter(sender);
  }

  /**
   * In {@code Flink 1.9} the {@link Mailbox} is not exposed yet to the users {@link
   * AbstractStreamOperator} but it is defined and functional in the containing {@link StreamTask}.
   */
  private static MailboxSender obtainContainingStreamTaskMailboxSenderForFlink19(
      StreamTask<?, ?> task) {
    try {
      Field field = StreamTask.class.getDeclaredField("mailbox");
      field.setAccessible(true);
      return (MailboxSender) field.get(task);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(
          "Stateful Functions is currently only supported with Flink 1.9 version.", e);
    }
  }

  private static final class MailboxAdapter implements Executor {
    private final MailboxSender sender;

    MailboxAdapter(MailboxSender sender) {
      this.sender = Objects.requireNonNull(sender);
    }

    @Override
    public void execute(@Nonnull Runnable command) {
      try {
        sender.putMail(command);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
