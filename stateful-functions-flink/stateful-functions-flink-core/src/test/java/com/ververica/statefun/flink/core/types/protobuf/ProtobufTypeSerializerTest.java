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

package com.ververica.statefun.flink.core.types.protobuf;

import com.ververica.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.Ignore;
import org.junit.Test;

public class ProtobufTypeSerializerTest extends SerializerTestBase<EnvelopeAddress> {

  @Override
  protected TypeSerializer<EnvelopeAddress> createSerializer() {
    return new ProtobufTypeSerializer<>(EnvelopeAddress.class);
  }

  @Ignore
  @Test()
  @Override
  public void testInstantiate() {
    // do nothing.
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<EnvelopeAddress> getTypeClass() {
    return EnvelopeAddress.class;
  }

  @Override
  protected EnvelopeAddress[] getTestData() {
    return new EnvelopeAddress[] {
      EnvelopeAddress.newBuilder().setType("").setNamespace("").setId("").build(),
      EnvelopeAddress.newBuilder().setType("").setNamespace("").setId("").build(),
      EnvelopeAddress.newBuilder().setType("").setNamespace("").setId("").build(),
      EnvelopeAddress.newBuilder().setType("").setNamespace("").setId("").build()
    };
  }
}
