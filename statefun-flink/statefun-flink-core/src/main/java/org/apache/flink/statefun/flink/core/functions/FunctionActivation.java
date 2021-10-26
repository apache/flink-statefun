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

import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.flink.statefun.flink.core.functions.utils.LaxityComparableObject;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message based implmentation
 */
public final class FunctionActivation extends LaxityComparableObject {
  public final PriorityQueue<Message> mailbox;
  private Address self;
  public LiveFunction function;
  private PriorityObject priority;
  private static final Logger LOG = LoggerFactory.getLogger(FunctionActivation.class);

  public FunctionActivation() {
    this.mailbox = new PriorityQueue<>((o1, o2) -> {
      try {
        return o1.getPriority().compareTo(o2.getPriority());
      } catch (Exception e) {
        e.printStackTrace();
      }
      return 0;
    });
    this.priority = null;
  }

  void setFunction(Address self, LiveFunction function) {
    this.self = self;
    this.function = function;
  }

  public void add(Message message) {
    mailbox.add(message);
    try {
      priority = mailbox.peek().getPriority();
    } catch (Exception e) {
      LOG.debug("Activation {} add message error {}", this, message);
      e.printStackTrace();
    }
  }

  public boolean hasPendingEnvelope() {
    return !mailbox.isEmpty();
  }

  void applyNextPendingEnvelope(ApplyingContext context) {
    try {
      Message message = mailbox.poll();
      if(!mailbox.isEmpty()) priority = mailbox.peek().getPriority();
      context.apply(function, message);
    } catch (Exception e) {
      LOG.debug("Activation {} applyNextPendingEnvelope error context {}", this, context);
      e.printStackTrace();
    }
  }

  public Message getNextPendingEnvelope() {
    try {
      Message polled = mailbox.poll();
      if(!mailbox.isEmpty()) priority = mailbox.peek().getPriority();
      return polled;
    } catch (Exception e) {
      LOG.debug("Activation {} getNextPendingEnvelope error ", this);
      e.printStackTrace();
    }
    return null;
  }

  void applyNextEnvelope(ApplyingContext context, Message message){
    context.apply(function, message);
  }

  public boolean removeEnvelope(Message envelope){
    return mailbox.remove(envelope);
  }

  public Address self() {
    return self;
  }

  @Override
  public String toString(){
    return String.format("[FunctionActivation %d address {%s} mailbox size {%d}]", this.hashCode(), (self==null?"null": self.toString()), mailbox.size());
  }

  public String toDetailedString(){
    return String.format("[FunctionActivation {%d} address {%s} LiveFunction {%s} mailbox size {%d} content {%s} priority {%s}]",
            this.hashCode(), (self==null?"null" :self.toString()), function.toString(), mailbox.size(), mailbox.stream().map(
                    Object::toString).collect(
                    Collectors.joining("|||")), priority==null?"null":priority.toString());
  }

  @Override
  public PriorityObject getPriority() throws Exception {
    return priority;
  }

  public void reset() {
    this.self = null;
    this.function = null;
    this.priority = null;
  }

  public ClassLoader getClassLoader (){
    return function.getClass().getClassLoader();
  }
}
