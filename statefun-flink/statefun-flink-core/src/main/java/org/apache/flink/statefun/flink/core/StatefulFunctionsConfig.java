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

import static org.apache.flink.configuration.description.TextElement.code;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst.StatefunPriorityBalancingLaxityCheckStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst.StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst.StatefunPriorityOnlyLaxityCheckStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.direct.StatefunCheckDirectLBStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.direct.StatefunCheckDirectQBStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.direct.StatefunCheckRangeMetaStateStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.forwardfirst.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.queuebased.ProactiveSchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.queuebased.ReactiveDummyStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.direct.StatefunCheckRangeDirectStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect.StatefunStatefulDirectFlushingStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.twolayer.aggregation.StatefunStatefulDirectLBStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect.StatefunStatefulRangeDirectStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive.StatefunStatefulStatelessFirstStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive.StatefunStatefulStatelessOnlyStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive.StatefunStatefulDirectForwardingStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive.StatefunStatefulFullMigrationStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.twolayer.aggregation.StatefunStatefulCheckAndInsertStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.twolayer.migration.StatefunStatefulMigrationStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.twolayer.migration.StatefunStatefulUpstreamMigrationStrategy;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;

/** Configuration that captures all stateful function related settings. */
@SuppressWarnings("WeakerAccess")
public class StatefulFunctionsConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String MODULE_CONFIG_PREFIX = "statefun.module.global-config.";

  // This configuration option exists for the documentation generator
  @SuppressWarnings("unused")
  public static final ConfigOption<String> MODULE_GLOBAL_DEFAULT =
      ConfigOptions.key(MODULE_CONFIG_PREFIX + "<KEY>")
          .stringType()
          .noDefaultValue()
          .withDescription(
              Description.builder()
                  .text(
                      "Adds the given key/value pair to the Stateful Functions global configuration.")
                  .text(
                      "These values will be available via the `globalConfigurations` parameter of StatefulFunctionModule#configure.")
                  .linebreak()
                  .text(
                      "Only the key <KEY> and value are added to the configuration. If the key/value pairs")
                  .list(
                      code(MODULE_CONFIG_PREFIX + "key1: value1"),
                      code(MODULE_CONFIG_PREFIX + "key2: value2"))
                  .text("are set, then the map")
                  .list(code("key1: value1"), code("key2: value2"))
                  .text("will be made available to your module at runtime.")
                  .build());

  public static final ConfigOption<MessageFactoryType> USER_MESSAGE_SERIALIZER =
      ConfigOptions.key("statefun.message.serializer")
          .enumType(MessageFactoryType.class)
          .defaultValue(MessageFactoryType.WITH_PROTOBUF_PAYLOADS)
          .withDescription("The serializer to use for on the wire messages.");

  public static final ConfigOption<String> USER_MESSAGE_CUSTOM_PAYLOAD_SERIALIZER_CLASS =
      ConfigOptions.key("statefun.message.custom-payload-serializer-class")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The custom payload serializer class to use with the WITH_CUSTOM_PAYLOADS serializer, which must implement MessagePayloadSerializer.");

  public static final ConfigOption<String> FLINK_JOB_NAME =
      ConfigOptions.key("statefun.flink-job-name")
          .stringType()
          .defaultValue("StatefulFunctions")
          .withDescription("The name to display at the Flink-UI");

  public static final ConfigOption<MemorySize> TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING =
      ConfigOptions.key("statefun.feedback.memory.size")
          .memoryType()
          .defaultValue(MemorySize.ofMebiBytes(32))
          .withDescription(
              "The number of bytes to use for in memory buffering of the feedback channel, before spilling to disk.");

  public static final ConfigOption<Integer> ASYNC_MAX_OPERATIONS_PER_TASK =
      ConfigOptions.key("statefun.async.max-per-task")
          .intType()
          .defaultValue(1024)
          .withDescription(
              "The max number of async operations per task before backpressure is applied.");

  public static final ConfigOption<String> STATFUN_SCHEDULING =
          ConfigOptions.key("statefun.scheduling")
                  .stringType()
                  .defaultValue("default")
                  .withDescription("Statefun scheduling strategy");

  public static final ConfigOption<Long> STATFUN_SCHEDULING_DELAY_THRESHOLD =
          ConfigOptions.key("statefun.scheduling.delay-threashold")
                  .longType()
                  .defaultValue(1500L)
                  .withDescription("Statefun scheduling delay threashold");
  public static final ConfigOption<Integer> STATFUN_SCHEDULING_QUEUE_SIZE_THRESHOLD =
          ConfigOptions.key("statefun.scheduling.queue-size-threshold")
                  .intType()
                  .defaultValue(2)
                  .withDescription("Statefun scheduling queue size threashold");
  public static final ConfigOption<Integer> STATFUN_SCHEDULING_OVERLOAD_THRESHOLD =
          ConfigOptions.key("statefun.scheduling.overload-threshold")
                  .intType()
                  .defaultValue(10)
                  .withDescription("Statefun scheduling overload threashold in number of items in queue");
  public static final ConfigOption<Integer> STATFUN_SCHEDULING_RESAMPLE_THRESHOLD =
          ConfigOptions.key("statefun.scheduling.resample-threshold")
                  .intType()
                  .defaultValue(1)
                  .withDescription("Statefun scheduling resample threshold in number of trials");
  public static final ConfigOption<Integer> STATFUN_SCHEDULING_SEARCH_RANGE =
          ConfigOptions.key("statefun.scheduling.search-range")
                  .intType()
                  .defaultValue(1)
                  .withDescription("Statefun scheduling number of potential candiates to contact");
  public static final ConfigOption<Integer> STATFUN_SCHEDULING_ID_SPAN =
          ConfigOptions.key("statefun.scheduling.id-span")
                  .intType()
                  .defaultValue(1)
                  .withDescription("Statefun scheduling max span of lessee exploration");
  public static final ConfigOption<Boolean> STATFUN_SCHEDULING_POLLING =
          ConfigOptions.key("statefun.scheduling.polling")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("Statefun select migrate target from head of queue");
  public static final ConfigOption<Integer> STATFUN_SCHEDULING_REPLY_REQUIRED =
          ConfigOptions.key("statefun.scheduling.reply-required")
                  .intType()
                  .defaultValue(1)
                  .withDescription("Statefun numer of replies required to end exploration");
  public static final ConfigOption<Boolean> STATFUN_SCHEDULING_FORCE_MIGRATE =
          ConfigOptions.key("statefun.scheduling.force-migrate")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("Statefun scheduling force migrating violation operators");
  public static final ConfigOption<Boolean> STATFUN_SCHEDULING_RANDOM_LESSEE =
          ConfigOptions.key("statefun.scheduling.random-lessee")
                  .booleanType()
                  .defaultValue(true)
                  .withDescription("Statefun scheduling select random lessee as migration target");

  public static final ConfigOption<Boolean> STATFUN_USE_DEFAULT_LAXITY_QUEUE =
          ConfigOptions.key("statefun.scheduling.default-queue")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("Statefun scheduling use default laxity queue");
  public static final ConfigOption<Long> STATEFUN_SCHEDULING_STRATEGY_QUANTUM =
          ConfigOptions.key("statefun.scheduling.strategy-quantum")
                  .longType()
                  .defaultValue(100L)
                  .withDescription("Statefun scheduling maximum strategy quantum in milliseconds");

  /**
   * Creates a new {@link StatefulFunctionsConfig} based on the default configurations in the
   * current environment set via the {@code flink-conf.yaml}.
   */
  public static StatefulFunctionsConfig fromEnvironment(StreamExecutionEnvironment env) {
    Configuration configuration = FlinkConfigExtractor.reflectivelyExtractFromEnv(env);
    return new StatefulFunctionsConfig(configuration);
  }

  public static StatefulFunctionsConfig fromFlinkConfiguration(Configuration flinkConfiguration) {
    return new StatefulFunctionsConfig(flinkConfiguration);
  }

  private MessageFactoryType factoryType;

  private String customPayloadSerializerClassName;

  private String flinkJobName;

  private HashMap<String, SchedulingStrategy> schedulers;

  private byte[] universeInitializerClassBytes;

  private MemorySize feedbackBufferSize;

  private int maxAsyncOperationsPerTask;

  private Map<String, String> globalConfigurations = new HashMap<>();

  private long strategyQuantum;

  /**
   * Create a new configuration object based on the values set in flink-conf.
   *
   * @param configuration a configuration to read the values from
   */
  private StatefulFunctionsConfig(Configuration configuration) {
    this.factoryType = configuration.get(USER_MESSAGE_SERIALIZER);
    this.customPayloadSerializerClassName =
        configuration.get(USER_MESSAGE_CUSTOM_PAYLOAD_SERIALIZER_CLASS);
    this.flinkJobName = configuration.get(FLINK_JOB_NAME);
    this.feedbackBufferSize = configuration.get(TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING);
    this.maxAsyncOperationsPerTask = configuration.get(ASYNC_MAX_OPERATIONS_PER_TASK);
    this.strategyQuantum = configuration.get(STATEFUN_SCHEDULING_STRATEGY_QUANTUM);
    String schedulingOptions = configuration.get(STATFUN_SCHEDULING);
    if(!schedulingOptions.equals("default")){
      schedulingOptions += ("," + STATFUN_SCHEDULING.defaultValue());
    }
    schedulers = new HashMap<>();
    String[] schedulingOptionsArr = schedulingOptions.split(",");
    for(String schedulingOption : schedulingOptionsArr){
      SchedulingStrategy scheduler;
      switch (schedulingOption){
        case "default":
          scheduler = new DefaultSchedulingStrategy();
          break;
        case "pb":
          scheduler = new PBSchedulingStrategy();
          break;
        case "proactive":
          scheduler = new ProactiveSchedulingStrategy();
          ((ProactiveSchedulingStrategy)scheduler).DELAY_THRESHOLD = configuration.get(STATFUN_SCHEDULING_DELAY_THRESHOLD);
          ((ProactiveSchedulingStrategy)scheduler).QUEUE_SIZE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_QUEUE_SIZE_THRESHOLD);
          ((ProactiveSchedulingStrategy)scheduler).OVERLOAD_THRESHOLD = configuration.get(STATFUN_SCHEDULING_OVERLOAD_THRESHOLD);
          break;
        case "reactivedummy":
          scheduler = new ReactiveDummyStrategy();
          ((ReactiveDummyStrategy)scheduler).DELAY_THRESHOLD = configuration.get(STATFUN_SCHEDULING_DELAY_THRESHOLD);
          ((ReactiveDummyStrategy)scheduler).QUEUE_SIZE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_QUEUE_SIZE_THRESHOLD);
          ((ReactiveDummyStrategy)scheduler).OVERLOAD_THRESHOLD = configuration.get(STATFUN_SCHEDULING_OVERLOAD_THRESHOLD);
          break;
        case "statefunpriorityonlylaxitycheck":
          scheduler = new StatefunPriorityOnlyLaxityCheckStrategy();
          ((StatefunPriorityOnlyLaxityCheckStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunPriorityOnlyLaxityCheckStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          ((StatefunPriorityOnlyLaxityCheckStrategy)scheduler).REPLY_REQUIRED = configuration.get(STATFUN_SCHEDULING_REPLY_REQUIRED);
          break;
        case "statefunpriorityonlylaxitycheckprogressiveexploration":
          scheduler = new StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy();
          ((StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          ((StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy)scheduler).REPLY_REQUIRED = configuration.get(STATFUN_SCHEDULING_REPLY_REQUIRED);
          break;
        case "statefunprioritybalancinglaxitycheck":
          scheduler = new StatefunPriorityBalancingLaxityCheckStrategy();
          ((StatefunPriorityBalancingLaxityCheckStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunPriorityBalancingLaxityCheckStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          break;
        case "statefunmessagelaxitycheck":
          scheduler = new StatefunMessageLaxityCheckStrategy();
          ((StatefunMessageLaxityCheckStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunMessageLaxityCheckStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunMessageLaxityCheckStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          ((StatefunMessageLaxityCheckStrategy)scheduler).USE_DEFAULT_LAXITY_QUEUE = configuration.get(STATFUN_USE_DEFAULT_LAXITY_QUEUE);
          break;
        case "statefuncheckandinsert":
          scheduler = new StatefunCheckAndInsertStrategy();
          ((StatefunCheckAndInsertStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunCheckAndInsertStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunCheckAndInsertStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          ((StatefunCheckAndInsertStrategy)scheduler).USE_DEFAULT_LAXITY_QUEUE = configuration.get(STATFUN_USE_DEFAULT_LAXITY_QUEUE);
          break;
        case "statefunrangeinsert":
          scheduler = new StatefunRangeInsertStrategy();
          ((StatefunRangeInsertStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunRangeInsertStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunRangeInsertStrategy)scheduler).ID_SPAN = configuration.get(STATFUN_SCHEDULING_ID_SPAN);
          ((StatefunRangeInsertStrategy)scheduler).POLLING = configuration.get(STATFUN_SCHEDULING_POLLING);
          ((StatefunRangeInsertStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunrangeinsertmetastate":
          scheduler = new StatefunRangeInsertMetaStateStrategy();
          ((StatefunRangeInsertMetaStateStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunRangeInsertMetaStateStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunRangeInsertMetaStateStrategy)scheduler).ID_SPAN = configuration.get(STATFUN_SCHEDULING_ID_SPAN);
          break;
        case "statefunrangelaxitycheck":
          scheduler = new StatefunRangeLaxityCheckStrategy();
          ((StatefunRangeLaxityCheckStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunRangeLaxityCheckStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunRangeLaxityCheckStrategy)scheduler).ID_SPAN = configuration.get(STATFUN_SCHEDULING_ID_SPAN);
          ((StatefunRangeLaxityCheckStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefulfullmigration":
          scheduler = new StatefunStatefulFullMigrationStrategy();
          ((StatefunStatefulFullMigrationStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunStatefulFullMigrationStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefuldirectforwarding":
          scheduler = new StatefunStatefulDirectForwardingStrategy();
          ((StatefunStatefulDirectForwardingStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunStatefulDirectForwardingStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefulstatelessfirst":
          scheduler = new StatefunStatefulStatelessFirstStrategy();
          ((StatefunStatefulStatelessFirstStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunStatefulStatelessFirstStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefulstatelessonly":
          scheduler = new StatefunStatefulStatelessOnlyStrategy();
          ((StatefunStatefulStatelessOnlyStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunStatefulStatelessOnlyStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunStatefulStatelessOnlyStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefuldirect":
          scheduler = new StatefunStatefulRangeDirectStrategy();
          ((StatefunStatefulRangeDirectStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          break;
        case "statefunstatefulflushingdirect":
          scheduler = new StatefunStatefulDirectFlushingStrategy();
          ((StatefunStatefulDirectFlushingStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          break;
        case "statefuncheckdirectQB":
          scheduler = new StatefunCheckDirectQBStrategy();
          ((StatefunCheckDirectQBStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          break;
        case "statefuncheckdirectLB":
          scheduler = new StatefunCheckDirectLBStrategy();
          ((StatefunCheckDirectLBStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          ((StatefunCheckDirectLBStrategy)scheduler).USE_DEFAULT_LAXITY_QUEUE = configuration.get(STATFUN_USE_DEFAULT_LAXITY_QUEUE);
          break;
        case "statefuncheckrangedirect":
          scheduler = new StatefunCheckRangeDirectStrategy();
          ((StatefunCheckRangeDirectStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          ((StatefunCheckRangeDirectStrategy)scheduler).ID_SPAN = configuration.get(STATFUN_SCHEDULING_ID_SPAN);
          break;
        case "statefunmetastaterangedirect":
          scheduler = new StatefunCheckRangeMetaStateStrategy();
          ((StatefunCheckRangeMetaStateStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          ((StatefunCheckRangeMetaStateStrategy)scheduler).ID_SPAN = configuration.get(STATFUN_SCHEDULING_ID_SPAN);
          ((StatefunCheckRangeMetaStateStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefullbdirect":
          scheduler = new StatefunStatefulDirectLBStrategy();
          ((StatefunStatefulDirectLBStrategy)scheduler).SEARCH_RANGE = configuration.get(STATFUN_SCHEDULING_SEARCH_RANGE);
          break;
        case "statefunstatefulcheckandinsert":
          scheduler = new StatefunStatefulCheckAndInsertStrategy();
          ((StatefunStatefulCheckAndInsertStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunStatefulCheckAndInsertStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunStatefulCheckAndInsertStrategy)scheduler).ID_SPAN = configuration.get(STATFUN_SCHEDULING_ID_SPAN);
          ((StatefunStatefulCheckAndInsertStrategy)scheduler).POLLING = configuration.get(STATFUN_SCHEDULING_POLLING);
          ((StatefunStatefulCheckAndInsertStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefulmigration":
          scheduler = new StatefunStatefulMigrationStrategy();
          ((StatefunStatefulMigrationStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
          ((StatefunStatefulMigrationStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
          ((StatefunStatefulMigrationStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        case "statefunstatefulupstreammigration":
          scheduler = new StatefunStatefulUpstreamMigrationStrategy();
//          ((StatefunStatefulUpstreamMigrationStrategy)scheduler).RESAMPLE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_RESAMPLE_THRESHOLD);
//          ((StatefunStatefulUpstreamMigrationStrategy)scheduler).FORCE_MIGRATE = configuration.get(STATFUN_SCHEDULING_FORCE_MIGRATE);
//          ((StatefunStatefulUpstreamMigrationStrategy)scheduler).RANDOM_LESSEE = configuration.get(STATFUN_SCHEDULING_RANDOM_LESSEE);
          break;
        default:
          scheduler = new ReactiveDummyStrategy();
          ((ReactiveDummyStrategy)scheduler).DELAY_THRESHOLD = configuration.get(STATFUN_SCHEDULING_DELAY_THRESHOLD);
          ((ReactiveDummyStrategy)scheduler).QUEUE_SIZE_THRESHOLD = configuration.get(STATFUN_SCHEDULING_QUEUE_SIZE_THRESHOLD);
          ((ReactiveDummyStrategy)scheduler).OVERLOAD_THRESHOLD = configuration.get(STATFUN_SCHEDULING_OVERLOAD_THRESHOLD);
      }
      schedulers.put(schedulingOption, scheduler);
    }


    for (String key : configuration.keySet()) {
      if (key.startsWith(MODULE_CONFIG_PREFIX)) {
        String value = configuration.get(ConfigOptions.key(key).stringType().noDefaultValue());
        String userKey = key.substring(MODULE_CONFIG_PREFIX.length());
        globalConfigurations.put(userKey, value);
      }
    }
  }

  /** Returns the factory type used to serialize messages. */
  public MessageFactoryType getFactoryType() {
    return factoryType;
  }

  /**
   * Returns the custom payload serializer class name, when factory type is WITH_CUSTOM_PAYLOADS *
   */
  public String getCustomPayloadSerializerClassName() {
    return customPayloadSerializerClassName;
  }

  /** Returns the factory key * */
  public MessageFactoryKey getFactoryKey() {
    return MessageFactoryKey.forType(this.factoryType, this.customPayloadSerializerClassName);
  }

  /** Sets the factory type used to serialize messages. */
  public void setFactoryType(MessageFactoryType factoryType) {
    this.factoryType = Objects.requireNonNull(factoryType);
  }

  /** Sets the custom payload serializer class name * */
  public void setCustomPayloadSerializerClassName(String customPayloadSerializerClassName) {
    this.customPayloadSerializerClassName = customPayloadSerializerClassName;
  }

  /** Returns the Flink job name that appears in the Web UI. */
  public String getFlinkJobName() {
    return flinkJobName;
  }

  /** Set the Flink job name that appears in the Web UI. */
  public void setFlinkJobName(String flinkJobName) {
    this.flinkJobName = Objects.requireNonNull(flinkJobName);
  }

  /** Returns the number of bytes to use for in memory buffering of the feedback channel. */
  public MemorySize getFeedbackBufferSize() {
    return feedbackBufferSize;
  }

  /** Sets the number of bytes to use for in memory buffering of the feedback channel. */
  public void setFeedbackBufferSize(MemorySize size) {
    this.feedbackBufferSize = Objects.requireNonNull(size);
  }

  public HashMap<String, SchedulingStrategy> getScheduler() {
    return schedulers;
  }

  public long getSchedulingQuantum(){
    return strategyQuantum;
  }

  /** Returns the max async operations allowed per task. */
  public int getMaxAsyncOperationsPerTask() {
    return maxAsyncOperationsPerTask;
  }

  /** Sets the max async operations allowed per task. */
  public void setMaxAsyncOperationsPerTask(int maxAsyncOperationsPerTask) {
    this.maxAsyncOperationsPerTask = maxAsyncOperationsPerTask;
  }

  /**
   * Retrieves the universe provider for loading modules.
   *
   * @param cl The classloader on which the provider class is located.
   * @return A {@link StatefulFunctionsUniverseProvider}.
   */
  public StatefulFunctionsUniverseProvider getProvider(ClassLoader cl) {
    try {
      return InstantiationUtil.deserializeObject(universeInitializerClassBytes, cl, false);
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Unable to initialize.", e);
    }
  }

  /** Sets the universe provider used to load modules. */
  public void setProvider(StatefulFunctionsUniverseProvider provider) {
    try {
      universeInitializerClassBytes = InstantiationUtil.serializeObject(provider);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Returns the global configurations passed to {@link
   * org.apache.flink.statefun.sdk.spi.StatefulFunctionModule#configure(Map,
   * StatefulFunctionModule.Binder)}.
   */
  public Map<String, String> getGlobalConfigurations() {
    return Collections.unmodifiableMap(globalConfigurations);
  }

  /** Adds all entries in this to the global configuration. */
  public void addAllGlobalConfigurations(Map<String, String> globalConfigurations) {
    this.globalConfigurations.putAll(globalConfigurations);
  }

  /**
   * Adds the given key/value pair to the global configuration.
   *
   * @param key the key of the key/value pair to be added
   * @param value the value of the key/value pair to be added
   */
  public void setGlobalConfiguration(String key, String value) {
    this.globalConfigurations.put(key, value);
  }
}
