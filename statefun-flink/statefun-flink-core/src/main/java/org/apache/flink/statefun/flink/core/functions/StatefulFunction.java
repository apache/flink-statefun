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
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;

import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.BaseStatefulFunction;
import org.apache.flink.statefun.sdk.Context;

public final class StatefulFunction implements LiveFunction {
  public final org.apache.flink.statefun.sdk.StatefulFunction statefulFunction;
  private final FunctionTypeMetrics metrics;
  private final MessageFactory messageFactory;
  private final LocalFunctionGroup ownerFunctionGroup;

  StatefulFunction(
          org.apache.flink.statefun.sdk.StatefulFunction statefulFunction,
          FunctionTypeMetrics metrics,
          MessageFactory messageFactory,
          Lazy<LocalFunctionGroup> ownerFunctionGroup) {

    this.statefulFunction = Objects.requireNonNull(statefulFunction);
    this.metrics = Objects.requireNonNull(metrics);
    this.messageFactory = Objects.requireNonNull(messageFactory);
    this.ownerFunctionGroup = Objects.requireNonNull(ownerFunctionGroup.get());
  }

  @Override
  public void receive(Context context, Message message) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      ClassLoader targetClassLoader = statefulFunction.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(targetClassLoader);
      ownerFunctionGroup.lock.lock();
      Object payload = message.payload(messageFactory, targetClassLoader);
      ownerFunctionGroup.lock.unlock();
      statefulFunction.invoke(context, payload);
    } catch (Exception e) {
      throw new StatefulFunctionInvocationException(context.self().type(), e);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public boolean statefulSubFunction(Address addressDetails){
    if(statefulFunction!=null && statefulFunction instanceof BaseStatefulFunction){
      return ((BaseStatefulFunction)statefulFunction).statefulSubFunction(addressDetails);
    }
    return false;
  }

  public org.apache.flink.statefun.sdk.StatefulFunction getStatefulFunction(){
    return statefulFunction;
  }

  @Override
  public FunctionTypeMetrics metrics() {
    return metrics;
  }

  @Override
  public String getStrategyTag(Address address) {
    if(statefulFunction != null && statefulFunction instanceof BaseStatefulFunction){
      return ((BaseStatefulFunction)statefulFunction).getStrategyTag(address);
    }
    else{
      throw new StatefulFunctionInvocationException(address.type(),
              new Exception(String.format("getStrategyTag can only be applied to BaseStatefulFunction, current function %s", statefulFunction)));
    }
  }

  @Override
  public Integer getNumUpstreams(Address address) {
    if(statefulFunction != null && statefulFunction instanceof BaseStatefulFunction){
      return ((BaseStatefulFunction)statefulFunction).getNumUpstreams(address);
    }
    else{
      throw new StatefulFunctionInvocationException(address.type(),
              new Exception(String.format("getNumUpstreams can only be applied to BaseStatefulFunction, current function %s", statefulFunction)));
    }
  }
}
