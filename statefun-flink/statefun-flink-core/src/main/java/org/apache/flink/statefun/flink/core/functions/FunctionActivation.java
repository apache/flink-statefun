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
  public final ArrayList<Message> runnableMessages;
  private HashMap<InternalAddress, ArrayList<Message>> blocked;
  private HashSet<InternalAddress> unblockSet;
  private Address self;
  public LiveFunction function;
  private PriorityObject priority;
  private LocalFunctionGroup controller;
  private boolean readyToBlock;
  private static final Logger LOG = LoggerFactory.getLogger(FunctionActivation.class);

  private Status status;

  private boolean pendingStateRequest;

  public enum Status{
    RUNNABLE, // Running user message only
    BLOCKED, // All channels are blocked (and hence being buffered) to the mailbox
    EXECUTE_CRITICAL // Channel not yet open but executing the critical messages
  }

  public FunctionActivation(LocalFunctionGroup controller) {
    this.runnableMessages = new ArrayList<>();
    this.unblockSet = new HashSet<>();
    this.controller = controller;
    this.priority = null;
    this.blocked = new HashMap<>();
    this.status = Status.RUNNABLE;
    this.pendingStateRequest = false;
    this.readyToBlock = false;
  }

  void setFunction(Address self, LiveFunction function) {
    this.self = self;
    this.function = function;
  }


  public boolean add(Message message) {
    if(message.getMessageType() == Message.MessageType.NON_FORWARDING){
      System.out.println("Insert NON_FORWARDING " + message + " tid: " + Thread.currentThread().getName());
    }
    InternalAddress sourceAddress = new InternalAddress(message.source(), message.source().type().getInternalType());
    if(blocked.containsKey(sourceAddress)){
      blocked.get(sourceAddress).add(message);
      return false;
    }
    if(message.getMessageType() == Message.MessageType.NON_FORWARDING){
      System.out.println("Insert NON_FORWARDING without blocking " + message
              + " blocked input " + Arrays.toString(blocked.keySet().stream().toArray())
              + " tid: " + Thread.currentThread().getName());
    }

    try {
      if ((!message.isControlMessage() && !message.isStateManagementMessage()
              && message.getMessageType()!= Message.MessageType.NON_FORWARDING)
              && this.status == FunctionActivation.Status.BLOCKED) {
        throw new Exception("Cannot insert user message when mailbox is in BLOCKED status. Message: " + message + " blocked address " + Arrays.toString(blocked.keySet().toArray()));
      }

      runnableMessages.add(message);
      if(!runnableMessages.isEmpty()) priority = runnableMessages.get(0).getPriority();
    } catch (Exception e) {
      LOG.debug("Activation {} add message error {}", this, message);
      e.printStackTrace();
    }
    return true;
  }

  public void
  onSyncReceive(Message syncMessage, int numUpstreams){
    try {
        Address source = syncMessage.source();
        if(source.type().getInternalType() == null){
          throw new Exception("Cannot block default internal type, address: " + source);
        }
        InternalAddress addressMatch = new InternalAddress(source, source.type().getInternalType());
        PriorityQueue<Message> blockedMessages = new PriorityQueue<>();
        if(blocked.containsKey(addressMatch)){
          blockedMessages.add(syncMessage);
        }
        else{
          blocked.put(addressMatch, new ArrayList<>());
        }
        blocked.get(addressMatch).addAll(blockedMessages);
        if(blocked.size() == numUpstreams && status == Status.RUNNABLE){
          System.out.println("Mailbox " + self() + " ready to block " + " blocked size " + blocked.size() + " status " + status);
          this.readyToBlock = true;
        }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ArrayList<Message> onUnsyncReceive(){
    ArrayList<Message> ret = new ArrayList<>();
    try {
      // NOTE: UNSYNC does not need to wait for each source. A single UNSYNC can unblock all channels.
      for (ArrayList<Message> blockedMessageList : blocked.values()) {
        ret.addAll(blockedMessageList);
      }
      blocked.clear();
      unblockSet.clear();
      System.out.println("Mailbox " + self() + " set back to Runnable " + " readyToBlock " + readyToBlock);
      this.status = Status.RUNNABLE;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret;
  }

  public ArrayList<Message> executeCriticalMessages() {
    ArrayList<Message> criticalMessages = new ArrayList<>();
    try {
      // Iterate over the blocked HashMap and add the first message in the queue.
      System.out.println("Poll and schedule  blocked size " + blocked.size()
              + " pending: " + Arrays.toString(runnableMessages.stream().toArray())
              + " blocked messages " + blocked.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue().size()).collect(Collectors.joining("|||"))
              + " tid: " + Thread.currentThread().getName());
      for (Map.Entry<InternalAddress, ArrayList<Message>> kv : blocked.entrySet()) {
        Message head = kv.getValue().remove(0);
        runnableMessages.add(head);
        criticalMessages.add(head);
        System.out.print("Remove critical message: key " + kv.getKey() + " value: " + head);
      }
      System.out.println("executeCriticalMessages insert all critical messages size " + runnableMessages.size()
              + "executeCriticalMessages size " + criticalMessages.size() + " tid: " + Thread.currentThread().getName());
      this.status = Status.EXECUTE_CRITICAL;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return criticalMessages;
  }

  public boolean hasPendingEnvelope() {
    return !runnableMessages.isEmpty() || !blocked.isEmpty();
  }

  public boolean hasRunnableEnvelope() {
    return !runnableMessages.isEmpty();
  }

  void applyNextEnvelope(ApplyingContext context, Message message){
    context.apply(function, message);
  }

  Message pollNextEnvelope(ApplyingContext context){
    Message ret = runnableMessages.remove(0);
    if(ret.getMessageType() == Message.MessageType.NON_FORWARDING){
      System.out.println("pollNextEnvelope NON_FORWARDING " + ret + " tid: " + Thread.currentThread().getName());
    }
    return ret;
  }

  public boolean removeEnvelope(Message envelope){
    boolean ret = runnableMessages.remove(envelope);
    if(envelope.getMessageType() == Message.MessageType.NON_FORWARDING){
      System.out.println("pollNextEnvelope NON_FORWARDING " + ret + " tid: " + Thread.currentThread().getName());
    }
    return ret;
  }

  public void setPendingStateRequest(boolean flag) {
    this.pendingStateRequest = flag;
  }

  public boolean getPendingStateRequest() {
    return this.pendingStateRequest;
  }

  boolean isReadyToBlock(){
    return readyToBlock;
  }

  void resetReadyToBlock(){
    this.readyToBlock = false;
  }

  void setReadyToBlock(boolean readyToBlock){
    this.readyToBlock = readyToBlock;
  }

  void setStatus(Status status){
    this.status = status;
  }

  public Status getStatus(){
    return status;
  }

  public Address self() {
    return self;
  }

  @Override
  public String toString(){
    return String.format("[FunctionActivation %d address {%s} status %s readyToBlock %s mailbox size {%d}]",
            this.hashCode(),
            (self==null?"null": self.toString()),
            status == null? "null": status.toString(),
            readyToBlock,
            runnableMessages.size());
  }

  public String toDetailedString(){
    return String.format("[FunctionActivation {%d} address {%s} LiveFunction {%s} mailbox size {%d} content {%s} priority {%s}]",
            this.hashCode(), (self==null?"null" :self.toString()), function.toString(), runnableMessages.size(), runnableMessages.stream().map(
                    Object::toString).collect(
                    Collectors.joining("|||")), priority==null?"null":priority.toString());
  }

  @Override
  public PriorityObject getPriority() throws Exception {
    return priority;
  }

  public void reset() {
    if(hasRunnableEnvelope() || hasPendingEnvelope()){
      System.out.println("Trying to reset activation with non empty messages " + " pending " + Arrays.toString(runnableMessages.toArray()) + " blocked " + Arrays.toString(blocked.keySet().toArray()));
    }
    this.self = null;
    this.function = null;
    this.priority = null;
    this.status = null;
  }

  public ClassLoader getClassLoader (){
    return function.getClass().getClassLoader();
  }

  public Set<Address> getBlocked(){
    return blocked.keySet().stream().map(x->x.address).collect(Collectors.toSet());
  }
}

