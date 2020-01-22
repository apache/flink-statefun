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
package org.apache.flink.statefun.flink.core.spi;

public class Constants {
  private Constants() {}

  public static final String MODULE_DIRECTORY = "/opt/statefun/modules";
  public static final String FLINK_JOB_JAR_PATH = "/opt/flink/lib/statefun-flink-core.jar";
  public static final String STATEFUL_FUNCTIONS_PACKAGE = "org.apache.flink.statefun.";
  public static final String STATEFUL_FUNCTIONS_MODULE_NAME = "module.yaml";
}
