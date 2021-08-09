/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.common.json;

import java.io.IOException;
import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.util.TimeUtils;

public final class StateFunObjectMapper {

  public static ObjectMapper create() {
    final ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final SimpleModule module = new SimpleModule("statefun");
    module.addSerializer(Duration.class, new DurationJsonSerializer());
    module.addDeserializer(Duration.class, new DurationJsonDeserializer());
    module.addDeserializer(TypeName.class, new TypeNameJsonDeserializer());

    mapper.registerModule(module);
    return mapper;
  }

  private static final class DurationJsonSerializer extends JsonSerializer<Duration> {

    @Override
    public void serialize(
        Duration duration, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeString(TimeUtils.formatWithHighestUnit(duration));
    }
  }

  private static final class DurationJsonDeserializer extends JsonDeserializer<Duration> {
    @Override
    public Duration deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return TimeUtils.parseDuration(jsonParser.getText());
    }
  }

  private static final class TypeNameJsonDeserializer extends JsonDeserializer<TypeName> {
    @Override
    public TypeName deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return TypeName.parseFrom(jsonParser.getText());
    }
  }
}
