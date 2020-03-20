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
package org.apache.flink.statefun.flink.io.kinesis;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.flink.io.kinesis.polyglot.GenericKinesisSinkProvider;
import org.apache.flink.statefun.flink.io.kinesis.polyglot.RoutableProtobufKinesisSourceProvider;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;

@AutoService(FlinkIoModule.class)
public final class KinesisFlinkIOModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(KinesisIOTypes.UNIVERSAL_INGRESS_TYPE, new KinesisSourceProvider());
    binder.bindSinkProvider(KinesisIOTypes.UNIVERSAL_EGRESS_TYPE, new KinesisSinkProvider());
    binder.bindSourceProvider(
        PolyglotKinesisIOTypes.ROUTABLE_PROTOBUF_KINESIS_INGRESS_TYPE,
        new RoutableProtobufKinesisSourceProvider());
    binder.bindSinkProvider(
        PolyglotKinesisIOTypes.GENERIC_KINESIS_EGRESS_TYPE, new GenericKinesisSinkProvider());
  }
}
