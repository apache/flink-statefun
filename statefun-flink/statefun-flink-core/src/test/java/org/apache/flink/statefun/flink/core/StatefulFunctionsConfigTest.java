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
package org.apache.flink.statefun.flink.core;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.statefun.flink.core.exceptions.StatefulFunctionsInvalidConfigException;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

public class StatefulFunctionsConfigTest {

  @Test
  public void testSetConfigurations() {
    final String testName = "test-name";

    Configuration configuration = new Configuration();
    configuration.set(StatefulFunctionsConfig.FLINK_JOB_NAME, testName);
    configuration.set(
        StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER, MessageFactoryType.WITH_KRYO_PAYLOADS);
    configuration.set(
        StatefulFunctionsConfig.TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING,
        MemorySize.ofMebiBytes(100));
    configuration.set(StatefulFunctionsConfig.ASYNC_MAX_OPERATIONS_PER_TASK, 100);
    configuration.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        "org.apache.flink.statefun;org.apache.kafka;com.google.protobuf");
    configuration.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
    configuration.setString("statefun.module.global-config.key1", "value1");
    configuration.setString("statefun.module.global-config.key2", "value2");

    StatefulFunctionsConfig stateFunConfig = new StatefulFunctionsConfig(configuration);

    Assert.assertEquals(stateFunConfig.getFlinkJobName(), testName);
    Assert.assertEquals(stateFunConfig.getFactoryType(), MessageFactoryType.WITH_KRYO_PAYLOADS);
    Assert.assertEquals(stateFunConfig.getFeedbackBufferSize(), MemorySize.ofMebiBytes(100));
    Assert.assertEquals(stateFunConfig.getMaxAsyncOperationsPerTask(), 100);
    Assert.assertThat(
        stateFunConfig.getGlobalConfigurations(), Matchers.hasEntry("key1", "value1"));
    Assert.assertThat(
        stateFunConfig.getGlobalConfigurations(), Matchers.hasEntry("key2", "value2"));
  }

  @Test(expected = StatefulFunctionsInvalidConfigException.class)
  public void invalidStrictFlinkConfigsThrows() {
    Configuration configuration = new Configuration();
    new StatefulFunctionsConfig(configuration);
  }

  private static Configuration validConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set(StatefulFunctionsConfig.FLINK_JOB_NAME, "name");
    configuration.set(
        StatefulFunctionsConfig.USER_MESSAGE_SERIALIZER, MessageFactoryType.WITH_KRYO_PAYLOADS);
    configuration.set(
        StatefulFunctionsConfig.TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING,
        MemorySize.ofMebiBytes(100));
    configuration.set(StatefulFunctionsConfig.ASYNC_MAX_OPERATIONS_PER_TASK, 100);
    configuration.set(
        CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
        "org.apache.flink.statefun;org.apache.kafka;com.google.protobuf");
    configuration.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
    return configuration;
  }
}
