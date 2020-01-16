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

package com.ververica.statefun.sdk.kafka;

import com.ververica.statefun.sdk.EgressType;
import com.ververica.statefun.sdk.IngressType;

public final class Constants {
  public static final IngressType KAFKA_INGRESS_TYPE =
      new IngressType("com.ververica.statefun.sdk.kafka", "universal-kafka-connector");
  public static final EgressType KAFKA_EGRESS_TYPE =
      new EgressType("com.ververica.statefun.sdk.kafka", "universal-kafka-connector");
  public static final IngressType PROTOBUF_KAFKA_INGRESS_TYPE =
      new IngressType("com.ververica.statefun.sdk.kafka", "protobuf-kafka-connector");;

  private Constants() {}
}
