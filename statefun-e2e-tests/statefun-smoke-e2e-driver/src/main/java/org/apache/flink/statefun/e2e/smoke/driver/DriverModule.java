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
package org.apache.flink.statefun.e2e.smoke.driver;

import static org.apache.flink.statefun.e2e.smoke.common.Constants.IN;
import static org.apache.flink.statefun.e2e.smoke.common.Types.unpackVerificationResult;

import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.statefun.e2e.smoke.common.Constants;
import org.apache.flink.statefun.e2e.smoke.common.Ids;
import org.apache.flink.statefun.e2e.smoke.common.ModuleParameters;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.statefun.flink.io.datastream.SinkFunctionSpec;
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(StatefulFunctionModule.class)
public class DriverModule implements StatefulFunctionModule {
  public static final Logger LOG = LoggerFactory.getLogger(DriverModule.class);

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    ModuleParameters moduleParameters = ModuleParameters.from(globalConfiguration);
    LOG.info(moduleParameters.toString());

    Ids ids = new Ids(moduleParameters.getNumberOfFunctionInstances());

    binder.bindIngress(new SourceFunctionSpec<>(IN, new CommandFlinkSource(moduleParameters)));
    binder.bindIngressRouter(IN, new CommandRouter(ids));

    binder.bindEgress(new SinkFunctionSpec<>(Constants.OUT, new DiscardingSink<>()));

    SocketClientSink<TypedValue> client =
        new SocketClientSink<>(
            moduleParameters.getVerificationServerHost(),
            moduleParameters.getVerificationServerPort(),
            new VerificationResultSerializer(),
            3,
            true);
    binder.bindEgress(new SinkFunctionSpec<>(Constants.VERIFICATION_RESULT, client));
  }

  private static final class VerificationResultSerializer
      implements SerializationSchema<TypedValue> {

    @Override
    public byte[] serialize(TypedValue element) {
      try {
        VerificationResult result = unpackVerificationResult(element);
        ByteArrayOutputStream out = new ByteArrayOutputStream(result.getSerializedSize() + 8);
        result.writeDelimitedTo(out);
        return out.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
