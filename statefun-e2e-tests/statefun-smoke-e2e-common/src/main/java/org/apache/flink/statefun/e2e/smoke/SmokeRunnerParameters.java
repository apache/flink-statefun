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
package org.apache.flink.statefun.e2e.smoke;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("unused")
public final class SmokeRunnerParameters implements Serializable {

  private static final long serialVersionUID = 1;

  private int numberOfFunctionInstances = 1_000;
  private int commandDepth = 10;
  private int messageCount = 100_000;
  private int maxCommandsPerDepth = 3;
  private double stateModificationsPr = 0.4;
  private double sendPr = 0.9;
  private double sendAfterPr = 0.1;
  private double asyncSendPr = 0.1;
  private double noopPr = 0.2;
  private double sendEgressPr = 0.03;
  private int maxFailures = 1;
  private String verificationServerHost = "localhost";
  private int verificationServerPort = 5050;
  private boolean isAsyncOpSupported = false;
  private long randomGeneratorSeed = System.nanoTime();

  /** Creates an instance of ModuleParameters from a key-value map. */
  public static SmokeRunnerParameters from(Map<String, String> globalConfiguration) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper.convertValue(globalConfiguration, SmokeRunnerParameters.class);
  }

  public Map<String, String> asMap() {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.convertValue(this, new TypeReference<Map<String, String>>() {});
  }

  public int getNumberOfFunctionInstances() {
    return numberOfFunctionInstances;
  }

  public void setNumberOfFunctionInstances(int numberOfFunctionInstances) {
    this.numberOfFunctionInstances = numberOfFunctionInstances;
  }

  public int getCommandDepth() {
    return commandDepth;
  }

  public void setCommandDepth(int commandDepth) {
    this.commandDepth = commandDepth;
  }

  public int getMessageCount() {
    return messageCount;
  }

  public void setMessageCount(int messageCount) {
    this.messageCount = messageCount;
  }

  public int getMaxCommandsPerDepth() {
    return maxCommandsPerDepth;
  }

  public void setMaxCommandsPerDepth(int maxCommandsPerDepth) {
    this.maxCommandsPerDepth = maxCommandsPerDepth;
  }

  public double getStateModificationsPr() {
    return stateModificationsPr;
  }

  public void setStateModificationsPr(double stateModificationsPr) {
    this.stateModificationsPr = stateModificationsPr;
  }

  public double getSendPr() {
    return sendPr;
  }

  public void setSendPr(double sendPr) {
    this.sendPr = sendPr;
  }

  public double getSendAfterPr() {
    return sendAfterPr;
  }

  public void setSendAfterPr(double sendAfterPr) {
    this.sendAfterPr = sendAfterPr;
  }

  public double getAsyncSendPr() {
    return asyncSendPr;
  }

  public void setAsyncSendPr(double asyncSendPr) {
    this.asyncSendPr = asyncSendPr;
  }

  public double getNoopPr() {
    return noopPr;
  }

  public void setNoopPr(double noopPr) {
    this.noopPr = noopPr;
  }

  public double getSendEgressPr() {
    return sendEgressPr;
  }

  public void setSendEgressPr(double sendEgressPr) {
    this.sendEgressPr = sendEgressPr;
  }

  public void setMaxFailures(int maxFailures) {
    this.maxFailures = maxFailures;
  }

  public int getMaxFailures() {
    return maxFailures;
  }

  public String getVerificationServerHost() {
    return verificationServerHost;
  }

  public void setVerificationServerHost(String verificationServerHost) {
    this.verificationServerHost = verificationServerHost;
  }

  public int getVerificationServerPort() {
    return verificationServerPort;
  }

  public void setVerificationServerPort(int verificationServerPort) {
    this.verificationServerPort = verificationServerPort;
  }

  public boolean isAsyncOpSupported() {
    return isAsyncOpSupported;
  }

  public void setAsyncOpSupported(boolean asyncOpSupported) {
    isAsyncOpSupported = asyncOpSupported;
  }

  public long getRandomGeneratorSeed() {
    return randomGeneratorSeed;
  }

  public void setRandomGeneratorSeed(long randomGeneratorSeed) {
    this.randomGeneratorSeed = randomGeneratorSeed;
  }

  @Override
  public String toString() {
    return "ModuleParameters{"
        + "numberOfFunctionInstances="
        + numberOfFunctionInstances
        + ", commandDepth="
        + commandDepth
        + ", messageCount="
        + messageCount
        + ", maxCommandsPerDepth="
        + maxCommandsPerDepth
        + ", stateModificationsPr="
        + stateModificationsPr
        + ", sendPr="
        + sendPr
        + ", sendAfterPr="
        + sendAfterPr
        + ", asyncSendPr="
        + asyncSendPr
        + ", noopPr="
        + noopPr
        + ", sendEgressPr="
        + sendEgressPr
        + ", maxFailures="
        + maxFailures
        + ", verificationServerHost='"
        + verificationServerHost
        + '\''
        + ", verificationServerPort="
        + verificationServerPort
        + ", isAsyncOpSupported="
        + isAsyncOpSupported
        + ", randomGeneratorSeed="
        + randomGeneratorSeed
        + '}';
  }
}
