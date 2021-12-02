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

import java.awt.*;
import java.util.*;
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
  private HashMap<InternalAddress, PriorityQueue<Message>> blocked;
  private HashSet<InternalAddress> unblockSet;
  private Address self;
  public LiveFunction function;
  private PriorityObject priority;
  private LocalFunctionGroup controller;
  private static final Logger LOG = LoggerFactory.getLogger(FunctionActivation.class);


  public FunctionActivation(LocalFunctionGroup controller) {
    this.mailbox = new PriorityQueue<>((o1, o2) -> {
      try {
        return o1.getPriority().compareTo(o2.getPriority());
      } catch (Exception e) {
        e.printStackTrace();
      }
      return 0;
    });
    this.unblockSet = new HashSet<>();
    this.controller = controller;
    this.priority = null;
    this.blocked = new HashMap<>();
  }

  void setFunction(Address self, LiveFunction function) {
    this.self = self;
    this.function = function;
  }


  public boolean add(Message message) {

    if(message.getMessageType() != Message.MessageType.UNSYNC){
      InternalAddress sourceAddress = new InternalAddress(message.source(), message.source().type().getInternalType());
      if(blocked.containsKey(sourceAddress)){
        blocked.get(sourceAddress).add(message);
        return false;
      }
    }
    mailbox.add(message);
    boolean ret = true;
    try {
      if(message.getMessageType() == Message.MessageType.SYNC){
        Address source = message.source();
        if(source.type().getInternalType() == null){
          throw new Exception("Cannot block default internal type, address: " + source);
        }
        InternalAddress addressMatch = new InternalAddress(source, source.type().getInternalType());
        PriorityQueue<Message> blockedMessages = new PriorityQueue<>();
        boolean start = false;
        PriorityQueue<Message> pqCopy = new PriorityQueue<>(mailbox);
        Message readyMessage = pqCopy.poll();
        while (readyMessage!=null){
          if(readyMessage.equals(message)){
            start = true;
          }
          else if(start && addressMatch.equals(new InternalAddress(readyMessage.source(), readyMessage.source().type().getInternalType()))){
            blockedMessages.add(readyMessage);
          }
          readyMessage = pqCopy.poll();
        }
        for(Message blockCandidate : blockedMessages){
          mailbox.remove(blockCandidate);
          boolean remove = controller.getWorkQueue().remove(blockCandidate);
        }
        if(blocked.containsKey(addressMatch)){
          mailbox.remove(message);
          controller.getWorkQueue().remove(message);
          blockedMessages.add(message);
          ret = false;
        }
        else{
          blocked.put(addressMatch, new PriorityQueue<>());
        }
        blocked.get(addressMatch).addAll(blockedMessages);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      if(!mailbox.isEmpty()) priority = mailbox.peek().getPriority();
    } catch (Exception e) {
      LOG.debug("Activation {} add message error {}", this, message);
      e.printStackTrace();
    }
    return ret;
  }

  public boolean hasPendingEnvelope() {
    return !mailbox.isEmpty() || !blocked.isEmpty();
  }

  public boolean hasRunnableEnvelope() {
    return !mailbox.isEmpty();
  }

  void applyNextEnvelope(ApplyingContext context, Message message){
    context.apply(function, message);
  }

  public boolean removeEnvelope(Message envelope){
    if(!envelope.equals(mailbox.peek())){
      System.out.println("Removing message from the middle of the queue: activation: " + toDetailedString());
    }
    boolean ret = mailbox.remove(envelope);
    try {
      if(envelope.getMessageType() == Message.MessageType.UNSYNC){
        Address source = envelope.source();
        InternalAddress addressMatch = new InternalAddress(source, source.type().getInternalType());
        unblockSet.add(addressMatch);
        if(unblockSet.size() == controller.getContext().getParallelism()){
          for(InternalAddress unblockAddress : unblockSet){
            if(blocked.containsKey(unblockAddress)){
              PriorityQueue<Message> blockedMessages = blocked.remove(unblockAddress);
              for(Message message : blockedMessages){
                boolean success = add(message);
                if(success)  controller.getWorkQueue().add(message);
              }
              blockedMessages = blocked.containsKey(unblockAddress)?blocked.get(unblockAddress) : new PriorityQueue<>();
            }
          }
          unblockSet.clear();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret;
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

  public Set<Address> getBlocked(){
    return blocked.keySet().stream().map(x->x.address).collect(Collectors.toSet());
  }
}
