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

package com.ververica.statefun.flink.core.feedback;

import com.ververica.statefun.flink.core.message.Message;
import java.util.concurrent.Executor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

@Internal
public final class Feedback {

  public static void registerFeedbackConsumer(
      SubtaskFeedbackKey<Message> subtaskKey,
      StreamTask<?, ?> containingTask,
      Executor mailBoxExecutor,
      FeedbackConsumer<Message> consumer) {

    FeedbackChannelBroker broker = FeedbackChannelBroker.get();
    FeedbackChannel<Message> channel = broker.getChannel(subtaskKey);
    channel.registerConsumer(consumer, containingTask.getCheckpointLock(), mailBoxExecutor);
  }
}
