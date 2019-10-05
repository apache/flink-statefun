/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.docs.io.custom.flink;

import com.ververica.statefun.docs.io.custom.MyEgressSpec;
import com.ververica.statefun.docs.io.custom.flink.sink.MySinkFunction;
import com.ververica.statefun.flink.io.spi.SinkProvider;
import com.ververica.statefun.sdk.io.EgressSpec;
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
