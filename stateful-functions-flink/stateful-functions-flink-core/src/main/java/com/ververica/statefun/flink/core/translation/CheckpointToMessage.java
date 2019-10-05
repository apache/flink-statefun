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

package com.ververica.statefun.flink.core.translation;

import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.flink.core.message.MessageFactoryType;
import java.io.Serializable;
import java.util.function.LongFunction;

final class CheckpointToMessage implements Serializable, LongFunction<Message> {

  private static final long serialVersionUID = 1L;

  private final MessageFactoryType messageFactoryType;
  private transient MessageFactory factory;

  CheckpointToMessage(MessageFactoryType messageFactoryType) {
    this.messageFactoryType = messageFactoryType;
  }

  @Override
  public Message apply(long checkpointId) {
    return factory().from(checkpointId);
  }

  private MessageFactory factory() {
    if (factory == null) {
      factory = MessageFactory.forType(messageFactoryType);
    }
    return factory;
  }
}
