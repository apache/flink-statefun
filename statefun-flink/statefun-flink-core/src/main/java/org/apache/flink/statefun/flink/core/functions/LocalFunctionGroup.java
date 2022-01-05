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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.functions.procedures.StateAggregation;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.pool.SimplePool;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.statefun.flink.core.StatefulFunctionsConfig.STATFUN_SCHEDULING;

public final class LocalFunctionGroup {
  private final HashMap<InternalAddress, FunctionActivation> activeFunctions;
  private final SimplePool<LocalFunctionGroup, FunctionActivation> pool;
  private final FunctionRepository repository;
  private final ReusableContext context;
  private final HashMap<String, SchedulingStrategy> messageToStrategy;
  private final HashMap<SchedulingStrategy, HashSet<FunctionActivation>> strategyToFunctions;
  private final LinkedList<SchedulingStrategy> pendingStrategies;
  public final ReentrantLock lock;
  public final Condition notEmpty;
  private final Thread localExecutor;
  private final StateAggregation procedure;

  private static final Logger LOG = LoggerFactory.getLogger(LocalFunctionGroup.class);

  @Inject
  LocalFunctionGroup(
          @Label("function-repository") FunctionRepository repository,
          @Label("applying-context") ApplyingContext context,
          @Label("scheduler") HashMap<String, SchedulingStrategy> messageToStrategy,
          @Label("configuration") StatefulFunctionsConfig configuration
  ) {
    this.activeFunctions = new HashMap<>();

//    this.aggregationInfo = new HashMap<>();
    this.pool = new SimplePool<>(FunctionActivation::new, 1024);
    this.repository = Objects.requireNonNull(repository);
    this.context = (ReusableContext) Objects.requireNonNull(context);
    this.messageToStrategy = messageToStrategy;
    this.strategyToFunctions = new HashMap<>();
    this.pendingStrategies = new LinkedList<>();
    if(this.messageToStrategy.isEmpty()){
      this.messageToStrategy.put(STATFUN_SCHEDULING.defaultValue(), new DefaultSchedulingStrategy());
    }
    for(SchedulingStrategy strategy : messageToStrategy.values()) {
      strategy.initialize(this, context);
      strategy.createWorkQueue();
    }

    this.lock = new ReentrantLock();
    this.notEmpty = lock.newCondition();
    long strategyQuantum = configuration.getSchedulingQuantum();
    LOG.info("Initialize Strategy Map: {} Scheduling Quantum {}",
            messageToStrategy.entrySet().stream().map(kv->kv.getKey() + ":"+ kv.getValue()).collect(Collectors.joining("|||")),
            strategyQuantum
    );

    class messageProcessor implements Runnable{
      @Override
      public void run() {
        FunctionActivation activation = null;
        Message nextPending = null;
        SchedulingStrategy nextStrategy = null;
        long startTime = 0L;
        boolean newStrategy = true;
        while(true){
          lock.lock();
          try {
              if(nextStrategy == null){
                while ((nextStrategy = pendingStrategies.poll()) == null) {
                  notEmpty.await();
                }
                nextStrategy.setState(StrategyState.RUNNING);
              }
              if(newStrategy) {
                startTime = Instant.now().toEpochMilli();
                newStrategy = false;
              }
              //TODO insert state messages to mailbox?
//              FunctionActivation mailbox = findControlPendingMailbox(nextStrategy);
//              if(mailbox != null){
//                // Process control pending mailbox first
//                nextPending = mailbox.pollNextEnvelope(context);
//                System.out.println("Execute nextPending control message " + nextPending + " tid: " + Thread.currentThread().getName());
//              }
//              else{
                // Process next pending data message
                nextPending = nextStrategy.getNextMessage();

                System.out.println("Execute nextPending regular message " + nextPending + " tid: " + Thread.currentThread().getName());
                activation = nextPending.getHostActivation();
//                activation.removeEnvelope(nextPending);
                // TODO put this check back
                if(!activation.removeEnvelope(nextPending)){
                  throw new FlinkRuntimeException("Trying to remove a message that is not in the runnable message queue: " + nextPending + " tid: "+ Thread.currentThread().getName());
                }
//              }


          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
          finally {
            lock.unlock();
          }

          if(nextPending.getMessageType().equals(Message.MessageType.SUGAR_PILL)){
            LOG.debug("Swallowing sugar pill at msg: " + nextPending + " context " + context);
            getStrategy(nextPending.target()).propagate(nextPending);
            break;
          }
          else{
            preApply(activation, context, nextPending);
            lock.lock();
            try{
              if (!nextPending.isStateManagementMessage()){
                activation.applyNextEnvelope(context, nextPending);
              }
              else{
                procedure.handleStateManagementMessage(context, nextPending);
              }
            }
            finally {
              lock.unlock();
            }
            postApply(activation, context, nextPending);
          }

          lock.lock();
          try{
            if(nextStrategy.hasRunnableMessages()){
              if((Instant.now().toEpochMilli() - startTime) > strategyQuantum){
                //remaining work but expire
                if(!pendingStrategies.contains(nextStrategy)) {
                  pendingStrategies.add(nextStrategy);
                }
                nextStrategy.setState(StrategyState.RUNNABLE);
                newStrategy = true;
                nextStrategy = null;
              }
            }
            else{
              nextStrategy.setState(StrategyState.WAITING);
              newStrategy = true;
              nextStrategy = null;
            }
            if (!activation.hasPendingEnvelope()) {
              if(activation.self()!=null) {
                unRegisterActivation(activation);
              }
            }
          }
          finally {
            lock.unlock();
          }
        }
      }
    }
    this.localExecutor = new Thread(new messageProcessor());
    this.localExecutor.setName(Thread.currentThread().getName() + " (worker thread)");
    this.procedure = new StateAggregation(this);
    this.localExecutor.start();
  }



  public void enqueue(Message message) {
    lock.lock();
    try {
      if(message.isControlMessage()){
          // Add to mailbox but not strategy
          FunctionActivation activation = getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
          if (activation == null) {
            activation = newActivation(message.target());
            if(message.getMessageType() == Message.MessageType.SYNC){
              System.out.println(" Receive SYNC request " + message
                      + " tid: " + Thread.currentThread().getName());
              // Inform mailbox of a SYNC message and pass the number of numUpstreams for the mailbox to change state.
              //TODO fix parallelism
              activation.onSyncReceive(message, getContext().getParallelism());
            }
            else if(message.getMessageType() == Message.MessageType.UNSYNC){
              ArrayList<Message> unblockedMessages = activation.onUnsyncReceive();
              for(Message unblockedMessage : unblockedMessages){
                enqueue(unblockedMessage);
              }
            }
          }
          else{
            if(message.getMessageType() == Message.MessageType.SYNC){
              System.out.println(" Receive SYNC request " + message
                      + " tid: " + Thread.currentThread().getName());
              // Inform mailbox of a SYNC message and pass the number of numUpstreams for the mailbox to change state.
              activation.onSyncReceive(message, getContext().getParallelism());
            }
            else if(message.getMessageType() == Message.MessageType.UNSYNC){
              ArrayList<Message> unblockedMessages = activation.onUnsyncReceive();
              for(Message unblockedMessage : unblockedMessages){
                enqueue(unblockedMessage);
              }
            }
          }

          if(activation.isReadyToBlock()){
            System.out.println("Ready to block from enqueue: queue size " + activation.runnableMessages.size() + " head message " + (activation.hasRunnableEnvelope()?activation.runnableMessages.get(0):"null") + " tid: " + Thread.currentThread().getName());
          }

          if(activation.isReadyToBlock() && !activation.hasRunnableEnvelope()){
            activation.resetReadyToBlock();
            activation.setStatus(FunctionActivation.Status.BLOCKED);
            System.out.println("Set address "+ activation.self()+ " to BLOCKED in enqueue, blocked size: "+ activation.getBlocked().size()  + " tid: " + Thread.currentThread().getName());
            procedure.handleOnBlock(activation, message);
          }
      }
      else{
        // 2. Has no effect on mailbox, needs scheduler attention only
        if(message.isSchedulerCommand()){
          getStrategy(message.target()).enqueue(message);
          return;
        }

        // 3. Inserting message to queue
        FunctionActivation activation = getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
        boolean needsRecycled = false;
        if (activation == null) {
          activation = newActivation(message.target());
          message.setHostActivation(activation);
          if(!message.isStateManagementMessage()){
            boolean success = activation.add(message);
            // Add to strategy
            if(success) getStrategy(message.target()).enqueue(message);
            if(getPendingStrategies().size()>0) {
              notEmpty.signal();
            }
          }
          else{
            needsRecycled = true;
          }
        }
        else{
          message.setHostActivation(activation);
          if(!message.isStateManagementMessage()){
            boolean success = activation.add(message);
            if (success) getStrategy(message.target()).enqueue(message);
            if(getPendingStrategies().size()>0) notEmpty.signal();
          }
        }

        // Step 1, 4-5
        procedure.handleNonControllerMessage(message);

        // 6. deregister any non necessary messages
        if(needsRecycled && activation.self()!=null && !message.getHostActivation().hasPendingEnvelope()) {
          unRegisterActivation(message.getHostActivation());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally{
      lock.unlock();
    }
  }

  public void preApply(FunctionActivation activation, ApplyingContext context, Message message){
    lock.lock();
    try{
      context.preApply(activation.function, message);
    }
    finally{
      lock.unlock();
    }
  }

  public void postApply(FunctionActivation activation, ApplyingContext context, Message message){
    lock.lock();
    try {
      // 1. Apply policy execution first
      context.postApply(activation.function, message);

      // 2. Check whether on lessor (then check whether critical executions have completed
      // If completed, trigger unsync
      Address self = activation.self();
      if(self == null){
        System.out.println("postApply empty activation retrieving info message " + message + " activation " + activation);
      }

      if(message.getMessageType()== Message.MessageType.NON_FORWARDING){
        System.out.println("PostApply NON_FORWARDING message " + message + " activation " + activation + " pending: " + Arrays.toString(activation.runnableMessages.toArray()));
      }
      if (procedure.ifLessor(self)) {
        System.out.println("Activation status " + message.getHostActivation().getStatus() + " message: " + message
                + " tid: " + Thread.currentThread().getName()
        );

        if (activation.getStatus() == FunctionActivation.Status.EXECUTE_CRITICAL &&
                !activation.hasRunnableEnvelope()) {
          // Unblock self channel and send UNSYNC requests to the other partitions
          System.out.println("Set mailbox state back to RUNNABLE " + message + " tid: " + Thread.currentThread().getName());
          ArrayList<Message> unblockedMessages = activation.onUnsyncReceive();
          for (Message unblockedMessage : unblockedMessages) {
            enqueue(unblockedMessage);
          }
          System.out.println("Send unsync messages [" + activation + "] address [" + activation.self() + "] self [" + self + "] activation [" + activation + "] message: " + message);
          // Need to use address (rather tha mailbox) here since enqueues may have changed the mailbox state
          sendUnsyncMessages(self);
        }
      }

      // 3. If Blocking condition is met:
      // - change mailbox status to block
      // - run onblock logic
      if(activation.isReadyToBlock()){
        System.out.println("Ready to block: queue size " + activation.runnableMessages.size() + " head message " + (activation.hasRunnableEnvelope()? activation.runnableMessages.get(0):"null")+ " tid: " + Thread.currentThread().getName());
      }
      if(!activation.hasRunnableEnvelope()){
        System.out.println("Empty activation " + activation.runnableMessages.size()+ " ready to block " + activation.isReadyToBlock()  + " tid: " + Thread.currentThread().getName());
      }
      if(activation.isReadyToBlock() && !activation.hasRunnableEnvelope()){
        activation.resetReadyToBlock();
        activation.setStatus(FunctionActivation.Status.BLOCKED);
        System.out.println("Set address "+ activation.self()+ " to BLOCKED postApply " + " blocked size " + activation.getBlocked().size() + " tid: " + Thread.currentThread().getName());
        procedure.handleOnBlock(activation, message);
      }

      //4. postApply continues, clean all context
      context.reset();
    }
    finally{
      lock.unlock();
    }
  }

  public Message prepareSend(Message message){
    //TODO add detailed logic
    return getStrategy(message.target()).prepareSend(message);
  }

  private void sendUnsyncMessages(Address self) {
    // Send an UNSYNC message to all partitioned operators.
    List<Message> unsyncMessages = procedure.getUnsyncMessages(self);
    for(Message message : unsyncMessages){
      context.send(message);
    }
  }

  public LinkedList<SchedulingStrategy> getPendingStrategies(){
    return pendingStrategies;
  }

  boolean processNextEnvelope() {
    if(pendingStrategies.size() == 0){
      throw new FlinkRuntimeException("Calling next envelope on empty strategy queue.");
    }
    Message message = pendingStrategies.poll().getNextMessage();
    if (message.getHostActivation() == null) {
      return false;
    }
    message.getHostActivation().applyNextEnvelope(context, message);
    if(message.getHostActivation().self()!=null && !message.getHostActivation().hasPendingEnvelope()) {
      unRegisterActivation(message.getHostActivation());
    }
    return true;
  }

  public FunctionActivation newActivation(Address self) {
    LiveFunction function = null;
    if (!self.type().equals(FunctionType.DEFAULT)){
      function = repository.get(self.type());
    }
    FunctionActivation activation = pool.get(this);
    activation.setFunction(self, function);
    MailboxState state = repository.getStatus(self);
    if (state == null){
      activation.setStatus(FunctionActivation.Status.RUNNABLE);
      activation.setReadyToBlock(false);
    }
    else{
      activation.setStatus(state.status);
      activation.setReadyToBlock(state.readyToBlock);
      activation.setPendingStateRequest(state.pendingStateRequest);
      if(state.readyToBlock){
        System.out.println("Set readyToBlock to true while block size is " + activation.getBlocked().size());
      }
    }
    activeFunctions.put(new InternalAddress(self, self.type().getInternalType()), activation);
    strategyToFunctions.putIfAbsent(getStrategy(self), new HashSet<>());
    strategyToFunctions.get(getStrategy(self)).add(activation);
    System.out.println("Create activation for address " + self + " activation "+ activation + " tid: " + Thread.currentThread().getName());
    return activation;
  }

  public void unRegisterActivation(FunctionActivation activation){
    System.out.println("Starting destroying activation "+ activation
            + " blocked size " + activation.getBlocked().size()
            + " tid: " + Thread.currentThread().getName());
    activeFunctions.remove(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
    strategyToFunctions.get(getStrategy(activation.self())).remove(activation);
    if(strategyToFunctions.get(getStrategy(activation.self())).isEmpty()){
      strategyToFunctions.remove(getStrategy(activation.self()));
    }
    repository.updateStatus(activation.self(), new MailboxState(activation.getStatus(), activation.isReadyToBlock(), activation.getPendingStateRequest()));
    activation.reset();
    pool.release(activation);
  }

  private FunctionActivation findControlPendingMailbox(SchedulingStrategy nextStrategy) {
    List<FunctionActivation> blockedFunctions = strategyToFunctions.get(nextStrategy).stream().filter(x->(x.getStatus() == FunctionActivation.Status.BLOCKED) && x.hasRunnableEnvelope()).collect(Collectors.toList());
    if(blockedFunctions.size() == 0) return null;
    return blockedFunctions.get(0);
  }

  public ClassLoader getClassLoader(Address target){
    if(target.equals(FunctionType.DEFAULT)) return null;
    return repository.get(target.type()).getClass().getClassLoader();
  }

  public int getPendingSize(){ return pendingStrategies.size(); }

  public ReusableContext getContext(){ return (ReusableContext) context; }

  public SchedulingStrategy getStrategy(Address address) {
    String tag = getFunction(address).getStrategyTag(address);
    if(tag == null || !(this.messageToStrategy.containsKey(tag))){
      tag = STATFUN_SCHEDULING.defaultValue();
    }
    System.out.println("tag " + (tag==null?"null":tag) + " Address " + address + " strategy map " + Arrays.toString(this.messageToStrategy.entrySet().stream().map(kv -> kv.getKey() + "->" + kv.getValue()).toArray()));
    return this.messageToStrategy.get(tag);
  }

  public StateAggregation getProcedure(){
    return procedure;
  }

  public HashMap<InternalAddress, FunctionActivation> getActiveFunctions() { return activeFunctions; }

  public LiveFunction getFunction(Address address) { return repository.get(address.type());}

  public void cancel(Message message){
    message.getHostActivation().removeEnvelope(message);
    if( !message.getHostActivation().hasPendingEnvelope()
            && message.getHostActivation().self()!=null){
      unRegisterActivation(message.getHostActivation());
    }
  }

  public void close() { }

}
