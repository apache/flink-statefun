/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.core.classloader;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;

/**
 * A variant of Flink's {@link SimpleUdfStreamOperatorFactory} that includes users statefun modules
 * on the user code classpath.
 *
 * @param <OUT> The output type of the operator
 */
@SuppressWarnings({"deprecation", "unchecked", "rawtypes"})
public class ModuleAwareUdfStreamOperatorFactory<OUT> implements UdfStreamOperatorFactory<OUT> {

  private static final long serialVersionUID = 1L;

  public static <OUT> ModuleAwareUdfStreamOperatorFactory<OUT> of(
      StatefulFunctionsConfig configuration, AbstractUdfStreamOperator<OUT, ?> operator) {
    SerializedValue<AbstractUdfStreamOperator<OUT, ?>> serializedValue =
        SerializedValue.of(operator);
    ModuleAwareUdfStreamOperatorFactory<OUT> factory =
        new ModuleAwareUdfStreamOperatorFactory<>(
            configuration,
            serializedValue,
            operator,
            operator.getClass(),
            operator instanceof StreamSource,
            operator instanceof InputTypeConfigurable);

    factory.setChainingStrategy(operator.getChainingStrategy());
    return factory;
  }

  private final StatefulFunctionsConfig configuration;

  private final SerializedValue<AbstractUdfStreamOperator<OUT, ?>> serializedOperator;

  @Nullable private final transient Class<?> type;

  private final boolean isStreamSource;

  private final boolean isInputTypeConfigurable;

  private ChainingStrategy chainingStrategy;

  @Nullable private SerializedValue<TypeInformation<?>> inputType;

  @Nullable private SerializedValue<TypeInformation<OUT>> outputType;

  private final String functionName;

  @Nullable private final transient Function userFunction;

  private ModuleAwareUdfStreamOperatorFactory(
      StatefulFunctionsConfig configuration,
      SerializedValue<AbstractUdfStreamOperator<OUT, ?>> serializedOperator,
      AbstractUdfStreamOperator<OUT, ?> operator,
      Class<?> type,
      boolean isStreamSource,
      boolean isInputTypeConfigurable) {

    this.configuration = Objects.requireNonNull(configuration);
    this.serializedOperator = Objects.requireNonNull(serializedOperator);
    this.type = Objects.requireNonNull(type);
    this.isStreamSource = isStreamSource;
    this.isInputTypeConfigurable = isInputTypeConfigurable;
    this.chainingStrategy = operator.getChainingStrategy();
    this.functionName = operator.getUserFunction().getClass().getName();
    this.userFunction = operator.getUserFunction();
  }

  @Override
  public <T extends StreamOperator<OUT>> T createStreamOperator(
      StreamOperatorParameters<OUT> parameters) {
    AbstractUdfStreamOperator<OUT, ?> operator;
    ClassLoader moduleClassLoader =
        ModuleClassLoader.createModuleClassLoader(
            configuration, parameters.getContainingTask().getUserCodeClassLoader());

    operator = serializedOperator.deserialize(moduleClassLoader);
    operator.setProcessingTimeService(parameters.getProcessingTimeService());
    operator.setChainingStrategy(chainingStrategy);
    operator.setup(
        parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());

    if (operator instanceof InputTypeConfigurable && inputType != null) {
      ((InputTypeConfigurable) operator)
          .setInputType(
              inputType.deserialize(moduleClassLoader),
              parameters.getContainingTask().getExecutionConfig());
    }

    if (outputType != null) {
      ((OutputTypeConfigurable<OUT>) operator)
          .setOutputType(
              outputType.deserialize(moduleClassLoader),
              parameters.getContainingTask().getExecutionConfig());
    }

    return (T) operator;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy strategy) {
    this.chainingStrategy = strategy;
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return this.chainingStrategy;
  }

  @Override
  public boolean isStreamSource() {
    return isStreamSource;
  }

  @Override
  public boolean isInputTypeConfigurable() {
    return isInputTypeConfigurable;
  }

  @Override
  public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
    this.inputType = SerializedValue.of(type);
  }

  @Override
  public boolean isOutputTypeConfigurable() {
    return true;
  }

  @Override
  public void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {
    this.outputType = SerializedValue.of(type);
  }

  @Override
  public String getUserFunctionClassName() {
    return functionName;
  }

  /**
   * @return user define function.
   * @throws RuntimeException As of Flink 1.11 this method is only ever called when validating the
   *     StreamGraph. Because the operator cannot be assumed to appear on the user code classloader
   *     the factory does not make any attempt to recover the function on the task managers and will
   *     instead throw.
   */
  @Override
  public Function getUserFunction() {
    if (userFunction == null) {
      throw new RuntimeException(
          "This method should only ever be called on the Dispatcher when generating"
              + " the Flink StreamGraph. This is bug, please file a ticket with the project maintainers");
    }
    return userFunction;
  }

  /**
   * @return the runtime class of the stream operator.
   * @throws RuntimeException As of Flink 1.11 this method is only ever called when validating the
   *     StreamGraph. Because the operator cannot be assumed to appear on the user code classloader
   *     the factory does not make any attempt to recover the class on the task managers and will
   *     instead throw.
   */
  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    if (type == null) {
      throw new RuntimeException(
          "This method should only ever be called on the Dispatcher when generating"
              + " the Flink StreamGraph. This is bug, please file a ticket with the project maintainers");
    }

    return (Class<? extends StreamOperator>) type;
  }
}
