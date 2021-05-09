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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.*;

import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReusableContext implements ApplyingContext, InternalContext {
  private final Partition thisPartition;
  private final LocalSink localSink;
  private final RemoteSink remoteSink;
  private final DelaySink delaySink;
  private final AsyncSink asyncSink;
  private final SideOutputSink sideOutputSink;
  public final State state;
  private final MessageFactory messageFactory;

  private Message in;
  private LiveFunction function;

  private ExecutorService service;
  private PersistedStateRegistry stateProvider;
  private LinkedBlockingDeque<Message> localSinkPendingQueue;
  private LinkedBlockingDeque<Message> remoteSinkPendingQueue;
  private Executor executor;
  //private LinkedBlockingDeque<Pair<EgressIdentifier<T>, T>> sideOutputSinkPendingQueue;
  public ArrayBlockingQueue<Runnable> taskQueue;
  private Lazy<LocalFunctionGroup> ownerFunctionGroup;
  private Long priority;
  private Long laxity;
  private Integer virtualizedIndex;
  private Object metaState;

  private static final Logger LOG = LoggerFactory.getLogger(ReusableContext.class);


  @Inject
  ReusableContext(
      Partition partition,
      LocalSink localSink,
      RemoteSink remoteSink,
      DelaySink delaySink,
      AsyncSink asyncSink,
      SideOutputSink sideoutputSink,
      @Label("state") State state,
      @Label("mailbox-executor") Executor operatorMailbox,
      @Label("function-group") Lazy<LocalFunctionGroup> localFunctionGroup,
      MessageFactory messageFactory) {

    this.thisPartition = Objects.requireNonNull(partition);
    this.localSink = Objects.requireNonNull(localSink);
    this.remoteSink = Objects.requireNonNull(remoteSink);
    this.delaySink = Objects.requireNonNull(delaySink);
    this.sideOutputSink = Objects.requireNonNull(sideoutputSink);
    this.state = Objects.requireNonNull(state);
    this.messageFactory = Objects.requireNonNull(messageFactory);
    this.asyncSink = Objects.requireNonNull(asyncSink);
    this.taskQueue = new ArrayBlockingQueue<Runnable>(100, true);
    this.localSinkPendingQueue = new LinkedBlockingDeque<>();
    this.remoteSinkPendingQueue = new LinkedBlockingDeque<>();
//    this.sideOutputSinkPendingQueue = new LinkedBlockingDeque<>();
    this.service = new ThreadPoolExecutor(1, 10,
            5000L, TimeUnit.MILLISECONDS,
            taskQueue, new ThreadPoolExecutor.CallerRunsPolicy());
    this.executor = operatorMailbox;
    this.ownerFunctionGroup = Objects.requireNonNull(localFunctionGroup);
    this.priority = null;
    this.laxity = null;
  }

  @Override
  public void apply(LiveFunction function, Message inMessage) {
    this.in = inMessage;
    this.function = function;
    try {
      setPriority(inMessage.getPriority().priority, inMessage.getPriority().laxity);
    } catch (Exception e) {
      e.printStackTrace();
    }
    long before = System.currentTimeMillis();
    //LOG.debug("ReusableContext " + getPartition().getThisOperatorIndex() + " Date " + new java.util.Date(before) + inMessage.target());
    if(function!=null) function.metrics().incomingMessage();
    ownerFunctionGroup.get().lock.lock();
    ownerFunctionGroup.get().getStrategy().preApply(inMessage);
    // LOG.debug("ReusableContext " + getPartition().getThisOperatorIndex() + " apply message " + inMessage ); //+ " queue " + ownerFunctionGroup.get().dumpWorkQueue());
    ownerFunctionGroup.get().lock.unlock();
    if (inMessage.isDataMessage() ||
            inMessage.getMessageType().equals(Message.MessageType.FORWARDED) ||
            inMessage.getMessageType().equals(Message.MessageType.REGISTRATION)
//            || inMessage.getMessageType().equals(Message.MessageType.gcontext.send(message.getLessor(), new SchedulerReply(successInsert, message.getMessageId(), message.source(), message.getLessor()), Message.MessageType.SCHEDULE_REPLY);
//           )
    ){
//      if(!inMessage.source().id().equals(Integer.toString(getPartition().getThisOperatorIndex()))){
//        System.out.println("ReusableContext " + getPartition().getThisOperatorIndex() + " forwarded " + " message " + inMessage);
//      }
      //LOG.debug("ReusableContext " + getPartition().getThisOperatorIndex() + " execute message " + inMessage);
      state.setCurrentKey(inMessage.target());
//      if(function == null){
//        System.out.println("ReusableContext " + getPartition().getThisOperatorIndex() + " function null " + " message " + inMessage);
//      }
      function.receive(this, in);
      in.postApply();
    }
    ownerFunctionGroup.get().lock.lock();
    ownerFunctionGroup.get().getStrategy().postApply(inMessage);
//    drainLocalSinkOutput();
//    drainRemoteSinkOutput();
    ownerFunctionGroup.get().lock.unlock();
//    executor.execute(this::drainLocalSinkOutput);
//    executor.execute(this::drainRemoteSinkOutput);
    long after = System.currentTimeMillis();
//    LOG.debug("ReusableContext " + getPartition().getThisOperatorIndex() + " Date " + new java.util.Date(after)
//            + " target " + inMessage.target() + " duration " + (after -before) + " queue size " + ownerFunctionGroup.get().getWorkQueue().size());
    this.metaState = null;
    this.in = null;
    this.priority = null;
    this.virtualizedIndex = null;
  }

  public void send(Address to, Object what, Message.MessageType type, PriorityObject priority){
    Objects.requireNonNull(to);
    Objects.requireNonNull(what);
    Message envelope = messageFactory.from(self(), to, what, priority.priority, priority.laxity, type);
    if (thisPartition.contains(to)) {
      localSinkPendingQueue.add(envelope);
      //executor.execute(this::drainLocalSinkOutput);
      drainLocalSinkOutput();
      if(function!=null) function.metrics().outgoingLocalMessage();
    } else {
      remoteSinkPendingQueue.add(envelope);
      //executor.execute(this::drainRemoteSinkOutput);
      drainRemoteSinkOutput();
      if(function!=null) function.metrics().outgoingRemoteMessage();
    }
  }

  public void forward(Address to, Message message, ClassLoader loader, boolean force){
    try {
      Objects.requireNonNull(to);
      Object what = message.payload(messageFactory, loader);
      Objects.requireNonNull(what);
      Address lessor = message.target();
      Message envelope = null;
      if(force){
        envelope = messageFactory.from(message.source(), to, what, message.getPriority().priority, message.getPriority().laxity,
                Message.MessageType.FORWARDED, message.getMessageId());
      }
      else{
        envelope = messageFactory.from(message.source(), to, what, message.getPriority().priority, message.getPriority().laxity,
                Message.MessageType.SCHEDULE_REQUEST, message.getMessageId());
      }
      envelope.setLessor(lessor);
//      LOG.debug("Context " + getPartition().getThisOperatorIndex() + " Forward message " + envelope + " original message " + message
//              + " lessor " + lessor + " contains in current partition " + thisPartition.contains(to));

      if (thisPartition.contains(to)) {
        localSinkPendingQueue.add(envelope);
        //executor.execute(this::drainLocalSinkOutput);
        drainLocalSinkOutput();
        //if(function!=null) function.metrics().outgoingLocalMessage();
      } else {
        remoteSinkPendingQueue.add(envelope);
        //executor.execute(this::drainRemoteSinkOutput);
        drainRemoteSinkOutput();
        //if(function!=null) function.metrics().outgoingRemoteMessage();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // User function needs lock
  @Override
  public void send(Address to, Object what) {
    ownerFunctionGroup.get().lock.lock();
    try {
      Objects.requireNonNull(to);
      Objects.requireNonNull(what);
      //Message envelope = messageFactory.from(self(), to, what, priority, laxity);
      Message envelope = ownerFunctionGroup.get().getStrategy()
              .prepareSend(messageFactory.from(self(), to, what, priority, laxity));
      if (envelope == null) return;
      if (thisPartition.contains(envelope.target())) {
        //localSink.accept(envelope);
        //executor.execute(this::drainLocalSinkOutput);
  //      ownerFunctionGroup.get().lock.lock();
          localSinkPendingQueue.add(envelope);
          drainLocalSinkOutput();
  //      ownerFunctionGroup.get().lock.unlock();
        if(function!=null) function.metrics().outgoingLocalMessage();
      } else {
        //remoteSink.accept(envelope);=
        //executor.execute(this::drainRemoteSinkOutput);
          remoteSinkPendingQueue.add(envelope);
          drainRemoteSinkOutput();
  //      ownerFunctionGroup.get().lock.unlock();
        if(function!=null) function.metrics().outgoingRemoteMessage();
      }
    }
    finally{
      ownerFunctionGroup.get().lock.unlock();
    }

  }


  public void send(Message envelope) {
    if (thisPartition.contains(envelope.target())) {
      //localSink.accept(envelope);
      localSinkPendingQueue.add(envelope);
      //executor.execute(this::drainLocalSinkOutput);
      drainLocalSinkOutput();
      if(function!=null) function.metrics().outgoingLocalMessage();
    } else {
      //remoteSink.accept(envelope);
      remoteSinkPendingQueue.add(envelope);
      //executor.execute(this::drainRemoteSinkOutput);
      drainRemoteSinkOutput();
      if(function!=null) function.metrics().outgoingRemoteMessage();
    }
  }

  // User function needs lock
  @Override
  public <T> void send(EgressIdentifier<T> egress, T what) {
    Objects.requireNonNull(egress);
    Objects.requireNonNull(what);
    ownerFunctionGroup.get().lock.lock();
    try {
      if (function != null) function.metrics().outgoingEgressMessage();
//    ownerFunctionGroup.get().lock.lock();
      sideOutputSink.accept(egress, what);
//    ownerFunctionGroup.get().lock.unlock();
    }
    finally{
      ownerFunctionGroup.get().lock.unlock();
    }
  }

  // User function needs lock
  @Override
  public void sendAfter(Duration delay, Address to, Object message) {
    Objects.requireNonNull(delay);
    Objects.requireNonNull(to);
    Objects.requireNonNull(message);

    Message envelope = messageFactory.from(self(), to, message, priority, laxity);
    ownerFunctionGroup.get().lock.lock();
    try {
      delaySink.accept(envelope, delay.toMillis());
    }
    finally{
      ownerFunctionGroup.get().lock.unlock();
    }
  }

  @Override
  public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {
    Objects.requireNonNull(metadata);
    Objects.requireNonNull(future);

    Message message = messageFactory.from(self(), self(), metadata, priority, laxity);
    try {
      asyncSink.accept(self(), message, future);
    }
    finally{
      ownerFunctionGroup.get().lock.unlock();
    }
  }

  @Override
  public PersistedStateRegistry getStateProvider() {
    return stateProvider;
  }

  @Override
  public void setStateProvider(PersistedStateRegistry provider) {
    stateProvider = provider;
  }

  @Override
  public ExecutorService getAsyncPool() {
    return service;
  }

  @Override
  public Object getMetaState() {
    return metaState;
  }

  @Override
  public Object setMetaState(Object state) {
    return metaState = state;
  }

  @Override
  public void drainLocalSinkOutput() {
    ArrayList<Message> pending = new ArrayList<>();
    localSinkPendingQueue.drainTo(pending);
//    System.out.println("ReuseableContext " +this.executor +  " drainLocalSinkOutput size " + pending.size() + " tid " + Thread.currentThread().getName());
    pending.forEach(m-> localSink.accept(m));

//    pending.clear();
//    sideOutputSinkPendingQueue.drainTo(pending);
//    pending.forEach(m->sideOutputSink.accept(m));
  }

  @Override
  public void drainRemoteSinkOutput() {
    ArrayList<Message> pending = new ArrayList<>();
    remoteSinkPendingQueue.drainTo(pending);
    pending.forEach(m-> remoteSink.accept(m));
  }

  @Override
  public void setPriority(Long priority) {
    if(in==null) throw new FlinkRuntimeException("You can't set priority out of context");
    try {
      this.priority = priority;
      this.laxity = 0L;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setPriority(Long priority, Long laxity) {
    if(in==null) throw new FlinkRuntimeException("You can't set priority out of context");
    try {
      this.priority = priority;
      this.laxity = laxity;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Long getPriority() {
    return this.priority;
  }

  public Long getLaxity() {
    return this.laxity;
  }

  @Override
  public void awaitAsyncOperationComplete() {
    asyncSink.blockAddress(self());
  }

  @Override
  public FunctionTypeMetrics functionTypeMetrics() {
    return function.metrics();
  }

  @Override
  public Address caller() {
    return in.source();
  }

  @Override
  public Address self() {
    return in.target();
  }

  public MessageFactory getMessageFactory(){ return messageFactory; }

  public Partition getPartition(){ return thisPartition; }

  public int getMaxParallelism(){
    return this.thisPartition.getMaxParallelism();
  }

  public int getParallelism(){
    return this.thisPartition.getParallelism();
  }

  public int getThisOperatorIndex(){
    return this.thisPartition.getThisOperatorIndex();
  }

  public int getVirtualizedIndex(){
    if(virtualizedIndex!=null) return virtualizedIndex;
    if(in==null){
      throw new FlinkRuntimeException("getVirtualizedIndex() should not be called out of context");
    }
    if(in.getMessageType().equals(Message.MessageType.FORWARDED)){
      LOG.debug("Context " + getPartition().getThisOperatorIndex() + " get forwarded message with lessor "  + in.getLessor()
              + " op id " + DataflowUtils.getPartitionId(in.getLessor().type().getInternalType()));
      return DataflowUtils.getPartitionId(in.getLessor().type().getInternalType());
    }
    else{
      LOG.debug("Context " + getPartition().getThisOperatorIndex() + " get regular message with target "  + in.target()
              + " op id " + DataflowUtils.getPartitionId(in.target().type().getInternalType()));
      return DataflowUtils.getPartitionId(in.target().type().getInternalType());
    }
  }

  public void setVirtualizedIndex(Integer storedVirtualizedIndex){
    this.virtualizedIndex = storedVirtualizedIndex;
  }

  int getDestinationOperatorIndex(FunctionType type, String id){
    return this.thisPartition.getDestinationOperatorIndex(new Address(type, id));
  }
}
