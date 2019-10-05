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
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.kafka.KafkaEgressBuilder;

public class EgressSpecs {

  public static final EgressIdentifier<User> ID =
      new EgressIdentifier<>("ververica", "output-egress", User.class);

  public static final EgressSpec<User> kafkaEgress =
      KafkaEgressBuilder.forIdentifier(ID)
          .withKafkaAddress("localhost:9092")
          .withSerializer(UserSerializer.class)
          .build();
}
