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

package com.ververica.statefun.flink.core;

import com.ververica.statefun.flink.core.generated.Envelope;
import com.ververica.statefun.flink.core.generated.EnvelopeAddress;
import com.ververica.statefun.flink.core.generated.Payload;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.flink.core.message.MessageFactoryType;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;

@SuppressWarnings("WeakerAccess")
public class TestUtils {

  public static final MessageFactory ENVELOPE_FACTORY =
      MessageFactory.forType(MessageFactoryType.WITH_KRYO_PAYLOADS);

  public static final FunctionType FUNCTION_TYPE = new FunctionType("test", "a");
  public static final Address FUNCTION_1_ADDR = new Address(FUNCTION_TYPE, "a-1");
  public static final Address FUNCTION_2_ADDR = new Address(FUNCTION_TYPE, "a-2");
  public static final EnvelopeAddress DUMMY_PAYLOAD = EnvelopeAddress.getDefaultInstance();

  public static final EnvelopeAddress ADDRESS =
      EnvelopeAddress.newBuilder().setNamespace("namespace").setType("type").setId("key-1").build();

  public static Address integerAddress(int i) {
    return new Address(new FunctionType("foo", "bar"), "bar-" + i);
  }

  public static Envelope[] envelopesOfVariousSizes() {
    Envelope[] envelopes = new Envelope[10];
    for (int i = 0; i < envelopes.length; i++) {
      envelopes[i] =
          Envelope.newBuilder()
              .setSource(ADDRESS)
              .setTarget(ADDRESS)
              .setPayload(
                  Payload.newBuilder()
                      .setClassName(EnvelopeAddress.class.getName())
                      .setPayloadBytes(ADDRESS.toByteString())
                      .build())
              .build();
    }
    return envelopes;
  }
}
