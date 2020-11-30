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

package org.apache.flink.statefun.flink.core.jsonmodule;

enum FormatVersion {
  v1_0("1.0"),
  v2_0("2.0"),
  v3_0("3.0");

  private String versionStr;

  FormatVersion(String versionStr) {
    this.versionStr = versionStr;
  }

  @Override
  public String toString() {
    return versionStr;
  }

  static FormatVersion fromString(String versionStr) {
    switch (versionStr) {
      case "1.0":
        return v1_0;
      case "2.0":
        return v2_0;
      case "3.0":
        return v3_0;
      default:
        throw new IllegalArgumentException("Unrecognized format version: " + versionStr);
    }
  }
}
