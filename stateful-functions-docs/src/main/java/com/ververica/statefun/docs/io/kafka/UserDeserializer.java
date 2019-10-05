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

package com.ververica.statefun.docs.io.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.statefun.docs.models.User;
import com.ververica.statefun.sdk.kafka.KafkaIngressDeserializer;
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserDeserializer implements KafkaIngressDeserializer<User> {

  private static Logger LOG = LoggerFactory.getLogger(UserDeserializer.class);

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public User deserialize(ConsumerRecord<byte[], byte[]> input) {
    try {
      return mapper.readValue(input.value(), User.class);
    } catch (IOException e) {
      LOG.debug("Failed to deserialize record", e);
      return null;
    }
  }
}
