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
package org.apache.flink.statefun.docs.io.custom.flink;

import org.apache.flink.statefun.docs.io.custom.MyEgressSpec;
import org.apache.flink.statefun.docs.io.custom.flink.sink.MySinkFunction;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MySinkProvider implements SinkProvider {

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> egressSpec) {
    MyEgressSpec<T> spec = asMyEgressSpec(egressSpec);
    MySinkFunction<T> sink = new MySinkFunction<>();

    // configure the sink based on the provided spec
    return sink;
  }

  private static <T> MyEgressSpec<T> asMyEgressSpec(EgressSpec<T> egressSpec) {
    if (egressSpec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }

    if (egressSpec instanceof MyEgressSpec) {
      return (MyEgressSpec<T>) egressSpec;
    }

    throw new IllegalArgumentException(String.format("Wrong type %s", egressSpec.type()));
  }
}
