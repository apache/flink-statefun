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
package org.apache.flink.statefun.flink.core.message;

import java.io.IOException;
import java.util.OptionalLong;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.sdk.Address;

public abstract class Message extends RoutableLaxityComparableObject {

   public enum MessageType{
    REQUEST,
    REPLY,
    INGRESS,
    EGRESS,
    SCHEDULE_REQUEST,
    SCHEDULE_REPLY,
    STAT_REQUEST,
    STAT_REPLY,
//    SYNC_ONE,
//    SYNC_ALL,
    SYNC,
    UNSYNC,
    FORWARDED,
    NON_FORWARDING,
    REGISTRATION,
    STATE_REGISTRATION,
    SUGAR_PILL,
    BARRIER,
    STATE_SYNC,
    STATE_SYNC_REPLY,
    STATE_AGGREGATE,
    STATE_REQUEST,
  }

  private FunctionActivation hostActivation;

  public void setHostActivation(FunctionActivation activation){
    hostActivation = activation;
  }

  public FunctionActivation getHostActivation(){
    return hostActivation;
  }

  public abstract Object payload(MessageFactory context, ClassLoader targetClassLoader);

  /**
   * isBarrierMessage - returns an empty optional for non barrier messages or wrapped checkpointId
   * for barrier messages.
   *
   * <p>When this message represents a checkpoint barrier, this method returns an {@code Optional}
   * of a checkpoint id that produced that barrier. For other types of messages (i.e. {@code
   * Payload}) this method returns an empty {@code Optional}.
   */
  public abstract OptionalLong isBarrierMessage();

  public abstract Message copy(MessageFactory context);

  public abstract void writeTo(MessageFactory context, DataOutputView target) throws IOException;

  public void postApply() {}

  public abstract void setPriority(Long priority, Long laxity) throws Exception;

  public abstract void setPriority(Long priority) throws Exception;

  public abstract MessageType getMessageType();

  public abstract void setMessageType(MessageType type);

  public boolean isDataMessage(){
      return (getMessageType()!=null) && (getMessageType().equals(MessageType.INGRESS)
              || getMessageType().equals(MessageType.REQUEST)
              || getMessageType().equals(MessageType.NON_FORWARDING)
      );
  }

  public boolean isControlMessage(){
      return getMessageType() == Message.MessageType.SYNC ||
              getMessageType() == Message.MessageType.UNSYNC ||
                getMessageType() == MessageType.STATE_REGISTRATION
              ;
//              getMessageType() == Message.MessageType.STATE_AGGREGATE ||
//              getMessageType() == Message.MessageType.STATE_REQUEST;
  }

  public boolean isStateManagementMessage(){
      return getMessageType() == MessageType.STATE_AGGREGATE ||
              getMessageType() == MessageType.STATE_REQUEST;
  }

  public boolean isSchedulerCommand(){
    return getMessageType() == MessageType.SCHEDULE_REQUEST ||
            getMessageType() == MessageType.SCHEDULE_REPLY;
  }

  public boolean isForwarded(){
      return getMessageType() == MessageType.FORWARDED;

  }

  public boolean isRegistration(){
      return getMessageType() == MessageType.REGISTRATION;
  }

  public abstract Long getMessageId();

  public abstract void setTarget(Address address);

  public abstract void setLessor(Address address);

  public abstract Address getLessor();
}
