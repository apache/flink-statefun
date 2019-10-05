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

import com.ververica.statefun.docs.models.User;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import com.ververica.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class IngressSpecs {

  public static final IngressIdentifier<User> ID =
      new IngressIdentifier<>(User.class, "ververica", "input-ingress");

  public static final IngressSpec<User> kafkaIngress =
      KafkaIngressBuilder.forIdentifier(ID)
          .withKafkaAddress("localhost:9092")
          .withTopic("my-topic")
          .withDeserializer(UserDeserializer.class)
          .withProperty(ConsumerConfig.GROUP_ID_CONFIG, "greetings")
          .build();
}
