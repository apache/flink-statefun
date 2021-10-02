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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.pool.SimplePool;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LocalFunctionGroup {
  private final HashMap<InternalAddress, FunctionActivation> activeFunctions;
  private final SimplePool<FunctionActivation> pool;
  private final FunctionRepository repository;
  private final ApplyingContext context;
  private final WorkQueue<Message> pending;
  private final SchedulingStrategy strategy;
  public final ReentrantLock lock;
  public final Condition notEmpty;
  private final Thread localExecutor;
  private static final Logger LOG = LoggerFactory.getLogger(LocalFunctionGroup.class);

  @Inject
  LocalFunctionGroup(
          @Label("function-repository") FunctionRepository repository,
          @Label("applying-context") ApplyingContext context,
          @Label("scheduler") SchedulingStrategy strategy
  ) {
    this.activeFunctions = new HashMap<>();
    this.pool = new SimplePool<>(FunctionActivation::new, 1024);
    this.repository = Objects.requireNonNull(repository);
    this.context = Objects.requireNonNull(context);
    this.strategy = strategy;
    strategy.initialize(this, context);
    this.pending = strategy.createWorkQueue();
    this.lock = new ReentrantLock();
    this.notEmpty = lock.newCondition();

    class messageProcessor implements Runnable{
      @Override
      public void run() {
        FunctionActivation activation = null;
        Message nextPending = null;
        while(true){
          lock.lock();
          try {
              while ((nextPending = pending.poll()) == null) {
                  notEmpty.await();
              }
              activation = nextPending.getHostActivation();
              activation.removeEnvelope(nextPending);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
          finally {
            lock.unlock();
          }

          if(nextPending.getMessageType().equals(Message.MessageType.SUGAR_PILL)){
            LOG.debug("Swallowing sugar pill at msg: " + nextPending + " context " + context);
            strategy.propagate(nextPending);
            break;
          }
          else{
            activation.applyNextEnvelope(context, nextPending);
          }

          lock.lock();
          try{
            if (!activation.hasPendingEnvelope()) {
              if(activation.self()!=null) {
                LOG.error("Unregister empty activation " + activation + " on message " + nextPending);
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
    this.localExecutor.start();
  }

  public void unRegisterActivation(FunctionActivation activation){
    activeFunctions.remove(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
    activation.reset();
    pool.release(activation);
  }

  public void enqueue(Message message) {
    strategy.enqueue(message);
  }

  public boolean enqueueWithCheck(Message message) {
    return strategy.enqueueWithCheck(message);
  }

  boolean processNextEnvelope() {
    Message message = pending.poll();
    if (message.getHostActivation() == null) {
      return false;
    }
    message.getHostActivation().applyNextEnvelope(context, message);
    if (!message.getHostActivation().hasPendingEnvelope()) {
      if(message.getHostActivation().self()!=null) unRegisterActivation(message.getHostActivation());
    }
    return true;
  }

  public FunctionActivation newActivation(Address self) {
    LiveFunction function = null;
    if (!self.type().equals(FunctionType.DEFAULT)){
      function = repository.get(self.type());
    }
    FunctionActivation activation = pool.get();
    activation.setFunction(self, function);
    activeFunctions.put(new InternalAddress(self, self.type().getInternalType()), activation);
    return activation;
  }

  public ClassLoader getClassLoader(Address target){
    if(target.equals(FunctionType.DEFAULT)) return null;
    return repository.get(target.type()).getClass().getClassLoader();
  }

  public int getPendingSize(){ return pending.size(); }

  public WorkQueue<Message> getWorkQueue() { return pending; }

  public SchedulingStrategy getStrategy() { return this.strategy; }

  public HashMap<InternalAddress, FunctionActivation> getActiveFunctions() { return activeFunctions; }

  public LiveFunction getFunction(Address address) { return repository.get(address.type());}

  public String dumpWorkQueue() {
    WorkQueue<Message> copy = pending.copy();
    String ret = "Priority Work Queue " + this.context  +" { \n";
    Message msg = copy.poll();
    while(msg!=null){
      try {
        ret += "-----" + msg.getPriority() + " : " + msg + "\n";
      } catch (Exception e) {
        e.printStackTrace();
      }
      msg = copy.poll();
    }
    ret+= "}\n";
    if(pending instanceof MinLaxityWorkQueue){
      ret += ((MinLaxityWorkQueue)pending).dumpLaxityMap();
    }
    return ret;
  }

  public void close() { }

}

/**
 * ActivationBase queue
 */
/*
public final class LocalFunctionGroup {
  //private final ObjectOpenHashMap<Address, FunctionActivation> activeFunctions;
  private final HashMap<InternalAddress, FunctionActivation> activeFunctions;
  //public final ArrayDeque<FunctionActivation> pending;
  private final SimplePool<FunctionActivation> pool;
  private final FunctionRepository repository;
  private final ApplyingContext context;
  //private final PriorityBlockingQueue<FunctionActivation> pending;
  private final WorkQueue<FunctionActivation> pending;
  private final SchedulingStrategy strategy;
  public final ReentrantLock lock;
  public final Condition notEmpty;
  private final Thread localExecutor;
  private static final Logger LOG = LoggerFactory.getLogger(LocalFunctionGroup.class);

  @Inject
  LocalFunctionGroup(
          @Label("function-repository") FunctionRepository repository,
          @Label("applying-context") ApplyingContext context,
          @Label("scheduler") SchedulingStrategy strategy
  ) {
    this.activeFunctions = new HashMap<>();
//    this.pending = new LinkedBlockingDeque<>();
    this.pool = new SimplePool<>(FunctionActivation::new, 1024);
    this.repository = Objects.requireNonNull(repository);
    this.context = Objects.requireNonNull(context);
    this.strategy = strategy;
    strategy.initialize(this, context);
    this.pending = strategy.createWorkQueue();
    this.lock = new ReentrantLock();
    this.notEmpty = lock.newCondition();

    class messageProcessor implements Runnable{
      @Override
      public void run() {
        FunctionActivation activation = null;
        Message nextPending = null;
        while(true){
          lock.lock();
          try {
//            System.out.println("LocalFunctionGroup processNextEnvelope " + this.toString() +  " tid " + Thread.currentThread().getName() + " activeFunctions size " + activeFunctions.size()
//                    + " time " + System.currentTimeMillis() +"  processNextEnvelope pending size " + pending.size() +" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));

            //activation = pending.take();
            while ((activation = pending.poll()) == null) {
              notEmpty.await();
            }

//            try {
//              System.out.println("LocalFunctionGroup run " + this.toString() + " tid " + Thread.currentThread().getName() +
//                      " activation " + activation + " time " + System.currentTimeMillis() + " pending size " + pending.size() + " queue: " + dumpWorkQueue());
//              nextPending = activation.getNextPendingEnvelope();
//            }
//              activation.applyNextEnvelope(context, nextPending);
//            synchronized (lock) {
//              System.out.println("LocalFunctionGroup run: " + this + " context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                      + " apply activation " + activation
//                      + " function " + (activation.function==null?"null":activation.function)
//                      + " mailbox size " + activation.mailbox.size()
//                      + " tid: " + Thread.currentThread().getName());
            //activation.applyNextPendingEnvelope(context);
            nextPending = activation.getNextPendingEnvelope();
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
          finally {
            lock.unlock();
          }
//          System.out.println("LocalFunctionGroup " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " process message " + (nextPending==null? "null":nextPending));

          if(nextPending.getMessageType().equals(Message.MessageType.SUGAR_PILL)){
            System.out.println("Swallowing sugar pill at msg: " + nextPending + " context " + context);
            strategy.propagate(nextPending);
            break;
          }
          else{
            System.out.println("Process next message at msg: " + nextPending + " context " + context);
            activation.applyNextEnvelope(context, nextPending);
          }

          //activation.applyNextPendingEnvelope(context);

          lock.lock();
          try{
              if (activation.hasPendingEnvelope()) {
                //pending.addLast(activation);
                //System.out.println("LocalFunctionGroup" + this.hashCode() + "  re-enqueue  activation " + activation + " queue " + dumpWorkQueue());
                if(!pending.contains(activation)){
                  pending.add(activation);
//                  System.out.println("LocalFunctionGroup run: " + this + " context " + context + " apply activation " + activation + " in queue, size: "
//                          + pending.size() + " mailbox size " + activation.mailbox.size()  + " tid: " + Thread.currentThread().getName());
                }

                // System.out.println("LocalFunctionGroup run: " + this + " context " + context +  " activation " + activation.self() + "  has remaining mails  " + activation.mailbox.size()  + " tid: " + Thread.currentThread().getName());
                //System.out.println("LocalFunctionGroup" + this.hashCode() + "  re-enqueue  activation " + activation);

              } else {
                //TODO: prevent strategy unregister activation -- add a flag
                if(activation.self()!=null)
                unRegisterActivation(activation);
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
    this.localExecutor.start();
  }

  public void unRegisterActivation(FunctionActivation activation){
    FunctionActivation removed = activeFunctions.remove(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
    // System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " unRegisterActivation " + activation);
    //System.out.println("LocalFunctionGroup before remove ActiveFunctions size " + activeFunctions.size() + " remove " + removed.self() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
    //System.out.println("LocalFunctionGroup run: " + this + " context " + context + "  remove  activation " + removed  + " tid: " + Thread.currentThread().getName());
    //System.out.println("LocalFunctionGroup" + this.hashCode() + "  after remove activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode() + " removed activation " + (removed==null?" null " : removed) +" ActiveFunctions size " + activeFunctions.size() + " : " + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
    //activation.setFunction(null, null);
    activation.reset();
    pool.release(activation);
  }

  //TODO
  void drainPendingOuputput(){
    context.drainLocalSinkOutput();
    context.drainRemoteSinkOutput();
  }

  public void enqueue(Message message) {
    lock.lock();
    try {
      FunctionActivation activation = activeFunctions.get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//      if(activation!=null && !activation.self().toString().contains(message.target().toString())){
//        System.out.println("LocalFunctionGroup enqueue  Message " + message
//                + " activeFunctions size " + activeFunctions.size()
//                + "Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                + " activation string " + (activation.self()==null? "null" : activation.self().toString())
//                + " target string " + (message.target()==null? "null": message.target().toString())
//                + " activation " +  (activation==null?"null":activation)); //" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));
//      }
//      LOG.debug("LocalFunctionGroup enqueue create activation " + (activation==null? " null ":activation)
//              + (message==null?" null " : message )
//              +  " pending queue size " + pending.size()
//              + " tid: "+ Thread.currentThread().getName() + " queue " + dumpWorkQueue());
      if (activation == null) {
        activation = newActivation(message.target());
        if(activation.function == null){
//          System.out.println("LocalFunctionGroup enqueue create activation with null function " + activation  + " message " + message);
        }

//      System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " new activation " + activation
//              + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
//              + " pending: " + pending.stream().map(x->x.toDetailedString()).collect(Collectors.joining("| |")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
        activation.add(message);
        pending.add(activation);
        //System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " LocalFunctionGroup enqueue create activation " + activation + " function " + (activation.function==null?"null": activation.function) + " message " + message);
        if(pending.size()>0) notEmpty.signal();
//        System.out.println("LocalFunctionGroup enqueue create activation " + (activation==null? " null ":activation)
//                + " mailbox " + activation.mailbox.size()
//                + " function " + activation.function
//                + (message==null?" null " : message )
//                +  " pending queue size " + pending.size()
//                +  " context " + context
//                + " tid: "+ Thread.currentThread().getName() );
//        System.out.println("LocalFunctionGroup enqueue " + this.toString()  + " Message " + message+
//                 " activation " +  (activation==null?"null":activation)+
//                 " queue " + dumpWorkQueue());
        return;
      }
//    System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " activation " + activation
//            + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
//            + " pending: " + pending.stream().map(x->x.toString()).collect(Collectors.joining("\n")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());

      if(pending.contains(activation)){
        pending.remove(activation);
        activation.add(message);
        pending.add(activation);
       //System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " LocalFunctionGroup re-enqueue create activation with null function " + activation  + " message " + message);
//        notEmpty.signal();
//        System.out.println("LocalFunctionGroup enqueue after dequeue create activation " + (activation==null? " null ":activation)
//                + " mailbox " + activation.mailbox.size()
//                + " function " + activation.function
//                + (message==null?" null " : message )
//                +  " pending queue size " + pending.size()
//                +  " context " + context
//                + " tid: "+ Thread.currentThread().getName() );
//        System.out.println("LocalFunctionGroup enqueue after dequeue " + this.toString() + " activation " + (activation==null?"null":activation)
//                +" " +  " queue " + dumpWorkQueue());
      }
      else{
        activation.add(message);
        pending.add(activation);
        notEmpty.signal();
//        System.out.println("LocalFunctionGroup enqueue as it is running create activation " + (activation==null? " null ":activation)
//                + " mailbox " + activation.mailbox.size()
//                + " function " + activation.function
//                + (message==null?" null " : message )
//                +  " pending queue size " + pending.size()
//                +  " context " + context
//                + " tid: "+ Thread.currentThread().getName() );
//        System.out.println("LocalFunctionGroup enqueue as it is running " + this.toString()   + " activation " + (activation==null?"null":activation)
//                +  " queue " + dumpWorkQueue()
//              );
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      lock.unlock();
    }
  }

  public boolean enqueueWithCheck(Message message) {
//    LOG.debug("LocalFunctionGroup enqueueWithCheck 1 message"
//            + (message==null?" null " : message )
//            +  " pending queue size " + pending.size()
//            + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
    if(!(pending instanceof MinLaxityWorkQueue)) {
      LOG.error("Should be using MinLaxityWorkQueue with this function.");
      return false;
    }
    MinLaxityWorkQueue<FunctionActivation> queue = (MinLaxityWorkQueue<FunctionActivation>)pending;
//    lock.lock();
    try {
      FunctionActivation activation = activeFunctions.get(new LocalFunctionGroup.InternalAddress(message.target(), message.target().type().getInternalType()));
//      LOG.debug("LocalFunctionGroup enqueueWithCheck 2 context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//              +" create activation " + (activation==null? " null ":activation)
//              + (message==null?" null " : message )
//              +  " pending queue size " + pending.size()
//              + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
      MinLaxityWorkQueue<FunctionActivation> workQueueCopy = (MinLaxityWorkQueue<FunctionActivation>) queue.copy();
      if (activation == null) {
        activation = newActivation(message.target());
        activation.add(message);
        if(queue.tryInsertWithLaxityCheck(activation)){
          if(queue.size()>0) notEmpty.signal();
//          LOG.debug("LocalFunctionGroup enqueueWithCheck 3.1 context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() +
//                  " success queue size " + workQueueCopy.size() + " -> " + queue.size());
          return true;
        }
        else{
          activation.mailbox.clear();
          unRegisterActivation(activation);
//          LOG.debug("LocalFunctionGroup enqueueWithCheck 3.2 context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() +
//                  " fail queue size " + workQueueCopy.size() + " -> " + queue.size());
          return false;
        }
      }
      if(queue.contains(activation)){
        queue.remove(activation);
        if(activation.getPriority().compareTo(message.getPriority()) < 0){
          // TODO
          activation.add(message);
          queue.add(activation);
//          LOG.debug("LocalFunctionGroup enqueueWithCheck 3.1 success activation exists context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() +
//                  " success queue size " + workQueueCopy.size() + " -> " + queue.size());
          return true;
        }
        else{
          activation.add(message);
          if(!queue.tryInsertWithLaxityCheck(activation)){
            activation.mailbox.remove(message);
            queue.add(activation);
//            LOG.debug("LocalFunctionGroup enqueueWithCheck 3.2 fail activation exists context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() +
//                    " success queue size " + workQueueCopy.size() + " -> " + queue.size());
            return false;
          }
//          LOG.debug("LocalFunctionGroup enqueueWithCheck 3.3 success activation exists context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() +
//                  " success queue size " + workQueueCopy.size() + " -> " + queue.size());
          return true;
        }
      }
      else{
//        LOG.debug("LocalFunctionGroup enqueueWithCheck 4 before activation not in queue context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " activation " + activation +
//                " queue detail " + dumpWorkQueue());
        activation.add(message);
        if(!queue.tryInsertWithLaxityCheck(activation)){
          activation.mailbox.remove(message);
//          LOG.debug("LocalFunctionGroup enqueueWithCheck 4.1 fail activation not in queue context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " activation " + activation +
//                  " success queue size " + workQueueCopy.size() + " -> " + queue.size() +  " queue detail " + dumpWorkQueue());
          return false;
        }
        else{
          notEmpty.signal();
//          LOG.debug("LocalFunctionGroup enqueueWithCheck 4.2 success activation not in queue context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " activation " + activation +
//                  " success queue size " + workQueueCopy.size() + " -> " + queue.size());
          return true;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
//    finally {
//      lock.unlock();
//    }
    return false;
  }

  boolean processNextEnvelope() {
//    System.out.println("LocalFunctionGroup processNextEnvelope " + this.toString() +  " tid " + Thread.currentThread().getName() + " activeFunctions size " + activeFunctions.size()
//            + " time " + System.currentTimeMillis() +"  processNextEnvelope pending size " + pending.size() +" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));
//            + " activeFunctions size " + activeFunctions.size() + " "
//            + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
    FunctionActivation activation = pending.poll();
    if (activation == null) {
      return false;
    }
    activation.applyNextPendingEnvelope(context);
    if (activation.hasPendingEnvelope()) {
      pending.add(activation);
//      System.out.println("LocalFunctionGroup processNextEnvelope " + this.hashCode() + "  re-enqueue  activation " + activation );

    } else {
      //System.out.println("LocalFunctionGroup before remove ActiveFunctions size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
      FunctionActivation removed = activeFunctions.remove(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
      //System.out.println("LocalFunctionGroup" + this.hashCode() + "  after remove activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode() + " removed activation " + (removed==null?" null " : removed) +" ActiveFunctions size " + activeFunctions.size() + " : " + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
      //activation.setFunction(null, null);
      activation.reset();
      pool.release(activation);
//      System.out.println("LocalFunctionGroup processNextEnvelope " + this.hashCode() + "  remove  activation " + activation + " queue " + dumpWorkQueue());
    }
    return true;
  }

  private FunctionActivation newActivation(Address self) {
    LiveFunction function = null;
    if (!self.type().equals(FunctionType.DEFAULT)){
      function = repository.get(self.type());
    }
    FunctionActivation activation = pool.get();
    activation.setFunction(self, function);
//    if(activation.function==null){
//          System.out.println("LocalFunctionGroup create new activation create null function for target address " + self  );
//    }
    activeFunctions.put(new InternalAddress(self, self.type().getInternalType()), activation);
//    System.out.println("LocalFunctionGroup create new activation " + (activation==null? " null ":activation)
//            +" LiveFunction " + function + " address " + self + " type key " + self.type() + " tid: " + Thread.currentThread().getName());
    return activation;
  }

  public int getPendingSize(){ return pending.size(); }

  public WorkQueue<FunctionActivation> getWorkQueue() { return pending; }

  public SchedulingStrategy getStrategy() { return this.strategy; }

  public String dumpWorkQueue() {
    WorkQueue<FunctionActivation> copy = pending.copy();
    String ret = "Priority Work Queue " + this.context  +" { \n";
    FunctionActivation fa = copy.poll();
    while(fa!=null){
      try {
        ret += "-----" + fa.getPriority() + " : " + fa.self() + "\n";
      } catch (Exception e) {
        e.printStackTrace();
      }
      fa = copy.poll();
    }
    ret+= "}\n";
    if(pending instanceof MinLaxityWorkQueue){
      ret += ((MinLaxityWorkQueue)pending).dumpLaxityMap();
    }
    return ret;
//    return String.format("Priority Work Queue " + this.context  +" { \n" + Arrays.toString(pending.stream().map(fa -> {
//      try {
//        return "-----" + fa.getPriority() + " : " + fa.self() + "\n";
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//      return "";
//    }).toArray()) + "}");
  }

  public void close() {
//    strategy.close();
//    try {
//      this.localExecutor.join();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
  }

  class InternalAddress{
    Address address;
    FunctionType internalType;

    InternalAddress(Address address, FunctionType internalType){
      this.address = address;
      this.internalType = internalType;
    }

    @Override
    public String toString(){
      return String.format("InternalAddress <%s, %s>", address.toString(), (internalType==null? "null":internalType.toString()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      InternalAddress internalAddress = (InternalAddress) o;
      return address.equals(internalAddress.address) &&
              ((internalType ==null && internalAddress.internalType==null)
                      || (internalType!=null && internalAddress.internalType!=null&& internalType.equals(internalAddress.internalType)));
    }

    @Override
    public int hashCode() {
      int hash = 0;
      hash = 37 * hash + address.hashCode();
      hash = 37 * hash + (internalType==null?0:internalType.hashCode());
      return hash;
    }
  }
}
*/

/**
 * Original
 */
/*
final class LocalFunctionGroup {
  //private final ObjectOpenHashMap<Address, FunctionActivation> activeFunctions;
  private final HashMap<InternalAddress, FunctionActivation> activeFunctions;
  //public final ArrayDeque<FunctionActivation> pending;
  private final SimplePool<FunctionActivation> pool;
  private final FunctionRepository repository;
  private final ApplyingContext context;
//  private final PriorityBlockingQueue<FunctionActivation> pending;
  private final LinkedBlockingDeque<FunctionActivation> pending;
  private SchedulingStrategy strategy;
  private Object lock;
  @Inject
  LocalFunctionGroup(
      @Label("function-repository") FunctionRepository repository,
      @Label("applying-context") ApplyingContext context) {
    this.activeFunctions = new HashMap<>();
    this.pending = new LinkedBlockingDeque<>();
//    this.pending = new PriorityBlockingQueue<>(1024, new Comparator<FunctionActivation>() {
//      @Override
//      public int compare(FunctionActivation o1, FunctionActivation o2) {
//        try {
//          return o1.getPriority().compareTo(Objects.requireNonNull(o2.getPriority()));
//        } catch (Exception e) {
//          e.printStackTrace();
//        }
//        return 0;
//      }
//    });
    this.pool = new SimplePool<>(FunctionActivation::new, 1024);
    this.repository = Objects.requireNonNull(repository);
    this.context = Objects.requireNonNull(context);
    this.strategy = new SchedulingStrategy(this, this.context);
    this.lock = new Object();
    class messageProcessor implements Runnable{
      @Override
      public void run() {
        while(true){
          try {
//            System.out.println("LocalFunctionGroup processNextEnvelope " + this.toString() +  " tid " + Thread.currentThread().getName() + " activeFunctions size " + activeFunctions.size()
//                    + " time " + System.currentTimeMillis() +"  processNextEnvelope pending size " + pending.size() +" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));
            FunctionActivation activation = pending.take();
            System.out.println("LocalFunctionGroup processNextEnvelope " + this.toString() +  " tid " + Thread.currentThread().getName() +
                    " activation " + activation + " time " + System.currentTimeMillis() + " pending size " + pending.size() + " queue: " + dumpWorkQueue());
            synchronized (lock){
              activation.applyNextPendingEnvelope(context);
              if (activation.hasPendingEnvelope()) {
                //pending.addLast(activation);
                // pending.add(activation);
                //System.out.println("LocalFunctionGroup" + this.hashCode() + "  re-enqueue  activation " + activation);
                  if(!pending.contains(activation)){
                      pending.add(activation);
                  }

              } else {
                //System.out.println("LocalFunctionGroup before remove ActiveFunctions size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
                FunctionActivation removed = activeFunctions.remove(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
                //System.out.println("LocalFunctionGroup" + this.hashCode() + "  after remove activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode() + " removed activation " + (removed==null?" null " : removed) +" ActiveFunctions size " + activeFunctions.size() + " : " + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
                activation.setFunction(null, null);
                pool.release(activation);
              }
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
    Thread thread = new Thread(new messageProcessor());
    thread.start();
  }

  //TODO
  void drainPendingOuputput(){
    context.drainLocalSinkOutput();
    context.drainRemoteSinkOutput();
  }


  void enqueue(Message message) {
    synchronized (lock){
      FunctionActivation activation = activeFunctions.get(new InternalAddress(message.target(), message.target().type().getInternalType()));
      System.out.println("LocalFunctionGroup enqueue " + this.toString() + " tid " + Thread.currentThread().getName() + " Message " + message
              + " activeFunctions size " + activeFunctions.size() + " activation " +  (activation==null?"null":activation)
              + " time " + System.currentTimeMillis() +"  processNextEnvelope pending size " + pending.size() +" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));
      if (activation == null) {
        activation = newActivation(message.target());
        //System.out.println("LocalFunctionGroup enqueue create activation " + activation + " target " + message.target());
//      System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " new activation " + activation
//              + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
//              + " pending: " + pending.stream().map(x->x.toDetailedString()).collect(Collectors.joining("| |")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
        activation.add(message);
        pending.add(activation);
        return;
      }
//    System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " activation " + activation
//            + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
//            + " pending: " + pending.stream().map(x->x.toString()).collect(Collectors.joining("\n")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
      activation.add(message);
//      if(pending.contains(activation)){
//        pending.remove(activation);
//        pending.add(activation);
//      }

      System.out.println("LocalFunctionGroup enqueue after enqueue " + this.toString() + " tid " + Thread.currentThread().getName() + " activeFunctions size " + activeFunctions.size() + " activation " + (activation==null?"null":activation)
              + " time " + System.currentTimeMillis() +"  processNextEnvelope pending size " + pending.size() +" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));
    }
  }

  boolean processNextEnvelope() {
    System.out.println("LocalFunctionGroup processNextEnvelope " + this.toString() +  " tid " + Thread.currentThread().getName() + " activeFunctions size " + activeFunctions.size()
            + " time " + System.currentTimeMillis() +"  processNextEnvelope pending size " + pending.size() +" " + pending.stream().map(x->x.toString()).collect(Collectors.joining("| |")));
//            + " activeFunctions size " + activeFunctions.size() + " "
//            + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
    FunctionActivation activation = pending.poll();
    if (activation == null) {
      return false;
    }
    activation.applyNextPendingEnvelope(context);
    if (activation.hasPendingEnvelope()) {
      pending.add(activation);
      //System.out.println("LocalFunctionGroup" + this.hashCode() + "  re-enqueue  activation " + activation);

    } else {
      //System.out.println("LocalFunctionGroup before remove ActiveFunctions size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
      FunctionActivation removed = activeFunctions.remove(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
      //System.out.println("LocalFunctionGroup" + this.hashCode() + "  after remove activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode() + " removed activation " + (removed==null?" null " : removed) +" ActiveFunctions size " + activeFunctions.size() + " : " + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']');
      activation.setFunction(null, null);
      pool.release(activation);
    }
    return true;
  }

  private FunctionActivation newActivation(Address self) {
    LiveFunction function = null;
    if (!self.type().equals(FunctionType.DEFAULT)){
      function = repository.get(self.type());
    }
    FunctionActivation activation = pool.get();
    activation.setFunction(self, function);
    activeFunctions.put(new InternalAddress(self, self.type().getInternalType()), activation);
    return activation;
  }

  public int getPendingSize(){
    return pending.size();
  }

  public SchedulingStrategy getStrategy() { return this.strategy; }

  private String dumpWorkQueue() {
    return String.format("Priority Work Queue " + this.context  +" { \n" + Arrays.toString(pending.stream().map(fa -> {
      try {
        return "-----" + fa.getPriority() + " : " + fa.self() + "\n";
      } catch (Exception e) {
        e.printStackTrace();
      }
      return "";
    }).toArray()) + "}");
  }

  class InternalAddress{
    Address address;
    FunctionType internalType;

    InternalAddress(Address address, FunctionType internalType){
      this.address = address;
      this.internalType = internalType;
    }

    @Override
    public String toString(){
      return String.format("InternalAddress <%s, %s>", address.toString(), (internalType==null? "null":internalType.toString()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      InternalAddress internalAddress = (InternalAddress) o;
      return address.equals(internalAddress.address) &&
              ((internalType ==null && internalAddress.internalType==null)
                      || (internalType!=null && internalAddress.internalType!=null&& internalType.equals(internalAddress.internalType)));
    }

    @Override
    public int hashCode() {
      int hash = 0;
      hash = 37 * hash + address.hashCode();
      hash = 37 * hash + (internalType==null?0:internalType.hashCode());
      return hash;
    }
  }
}
*/
