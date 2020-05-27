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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;

public final class Pointers {

  private Pointers() {}

  public static final JsonPointer FORMAT_VERSION = JsonPointer.compile("/version");

  // -------------------------------------------------------------------------------------
  // top level spec definition
  // -------------------------------------------------------------------------------------

  public static final JsonPointer MODULE_META_TYPE = JsonPointer.compile("/module/meta/type");
  public static final JsonPointer MODULE_SPEC = JsonPointer.compile("/module/spec");
  public static final JsonPointer FUNCTIONS_POINTER = JsonPointer.compile("/functions");
  public static final JsonPointer ROUTERS_POINTER = JsonPointer.compile("/routers");
  public static final JsonPointer INGRESSES_POINTER = JsonPointer.compile("/ingresses");
  public static final JsonPointer EGRESSES_POINTER = JsonPointer.compile("/egresses");

  // -------------------------------------------------------------------------------------
  // function
  // -------------------------------------------------------------------------------------

  public static final class Functions {
    public static final JsonPointer META_KIND = JsonPointer.compile("/function/meta/kind");
    public static final JsonPointer META_TYPE = JsonPointer.compile("/function/meta/type");
    public static final JsonPointer FUNCTION_HOSTNAME = JsonPointer.compile("/function/spec/host");
    public static final JsonPointer FUNCTION_ENDPOINT =
        JsonPointer.compile("/function/spec/endpoint");
    public static final JsonPointer FUNCTION_PORT = JsonPointer.compile("/function/spec/port");
    public static final JsonPointer FUNCTION_STATES = JsonPointer.compile("/function/spec/states");
    public static final JsonPointer FUNCTION_TIMEOUT =
        JsonPointer.compile("/function/spec/timeout");
    public static final JsonPointer FUNCTION_MAX_NUM_BATCH_REQUESTS =
        JsonPointer.compile("/function/spec/maxNumBatchRequests");
  }

  public static final class FunctionStates {
    public static final JsonPointer FUNCTION_STATE_NAME = JsonPointer.compile("/name");
    public static final JsonPointer FUNCTION_STATE_TTL_DURATION =
        JsonPointer.compile("/ttlDurationMillis");
  }

  // -------------------------------------------------------------------------------------
  // routers
  // -------------------------------------------------------------------------------------

  public static final class Routers {

    public static final JsonPointer META_TYPE = JsonPointer.compile("/router/meta/type");
    public static final JsonPointer SPEC_INGRESS = JsonPointer.compile("/router/spec/ingress");
    public static final JsonPointer SPEC_TARGET = JsonPointer.compile("/router/spec/target");
    public static final JsonPointer SPEC_DESCRIPTOR =
        JsonPointer.compile("/router/spec/descriptorSet");
    public static final JsonPointer SPEC_MESSAGE_TYPE =
        JsonPointer.compile("/router/spec/messageType");
  }

  // -------------------------------------------------------------------------------------
  // ingresses
  // -------------------------------------------------------------------------------------

  public static final class Ingress {

    public static final JsonPointer META_ID = JsonPointer.compile("/ingress/meta/id");
    public static final JsonPointer META_TYPE = JsonPointer.compile("/ingress/meta/type");
  }

  public static final class Egress {

    public static final JsonPointer META_ID = JsonPointer.compile("/egress/meta/id");
    public static final JsonPointer META_TYPE = JsonPointer.compile("/egress/meta/type");
  }
}
