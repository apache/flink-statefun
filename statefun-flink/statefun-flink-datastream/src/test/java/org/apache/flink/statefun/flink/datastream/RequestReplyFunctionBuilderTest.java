package org.apache.flink.statefun.flink.datastream;

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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;
import java.util.Random;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientSpec;
import org.apache.flink.util.TimeUtils;
import org.junit.Test;

public class RequestReplyFunctionBuilderTest {

  @Test
  public void serializeClientSpec() {

    final int seed = 27;
    final int maxSeconds = 300;
    final Random random = new Random(seed);
    final Duration readTimeout = Duration.ofSeconds(random.nextInt(maxSeconds));
    final Duration writeTimeout = Duration.ofSeconds(random.nextInt(maxSeconds));
    final Duration connectTimeout = Duration.ofSeconds(random.nextInt(maxSeconds));
    final Duration callTimeout = connectTimeout.plusSeconds(random.nextInt(maxSeconds));

    final DefaultHttpRequestReplyClientSpec.Timeouts timeouts =
        new DefaultHttpRequestReplyClientSpec.Timeouts();
    timeouts.setCallTimeout(callTimeout);
    timeouts.setReadTimeout(readTimeout);
    timeouts.setWriteTimeout(writeTimeout);
    timeouts.setConnectTimeout(connectTimeout);

    final ObjectNode rootNode =
        RequestReplyFunctionBuilder.transportClientPropertiesAsObjectNode(timeouts);
    assertThat(rootNode, notNullValue());
    final JsonNode timeoutsNode = rootNode.get("timeouts");
    assertThat(timeoutsNode, notNullValue());

    assertThat(timeoutsNode.size(), equalTo(4));
    assertThat(TimeUtils.parseDuration(timeoutsNode.get("read").asText()), equalTo(readTimeout));
    assertThat(TimeUtils.parseDuration(timeoutsNode.get("write").asText()), equalTo(writeTimeout));
    assertThat(
        TimeUtils.parseDuration(timeoutsNode.get("connect").asText()), equalTo(connectTimeout));
    assertThat(TimeUtils.parseDuration(timeoutsNode.get("call").asText()), equalTo(callTimeout));
  }
}
