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
package org.apache.flink.statefun.flink.state.processor;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.NewSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJobConstants;
import org.apache.flink.statefun.flink.state.processor.operator.FunctionsStateBootstrapOperator;
import org.apache.flink.statefun.flink.state.processor.operator.StateBootstrapFunctionRegistry;
import org.apache.flink.statefun.flink.state.processor.union.BootstrapDataset;
import org.apache.flink.statefun.flink.state.processor.union.BootstrapDatasetUnion;
import org.apache.flink.statefun.flink.state.processor.union.TaggedBootstrapData;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.util.Preconditions;

/**
 * Entry point for generating a new savepoint for a Stateful Functions application.
 *
 * <p>Users register multiple {@link StateBootstrapFunction}s that each define how to bootstrap a
 * given stateful function, as well as provide Flink {@link DataSet}s that contain data that serve
 * as input for the bootstrap functions. The {@code StatefulFunctionsSavepointCreator} can then be
 * used to construct a Flink batch job which writes out a savepoint that contains the bootstrapped
 * state and may be used to restore a Stateful Functions application.
 */
public class StatefulFunctionsSavepointCreator {

  private final int maxParallelism;

  private StateBackend stateBackend;

  private final StateBootstrapFunctionRegistry stateBootstrapFunctionRegistry =
      new StateBootstrapFunctionRegistry();

  private final List<BootstrapDataset<?>> bootstrapDatasets = new LinkedList<>();

  /**
   * Creates a {@link StatefulFunctionsSavepointCreator}.
   *
   * @param maxParallelism max parallelism of the Stateful Functions application to be restored
   *     using the generated savepoint.
   */
  public StatefulFunctionsSavepointCreator(int maxParallelism) {
    Preconditions.checkArgument(maxParallelism > 0);
    this.maxParallelism = maxParallelism;

    try {
      this.stateBackend = new RocksDBStateBackend("file:///tmp/ignored");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Use Flink's {@link FsStateBackend} to generate the savepoint. By default, {@link
   * RocksDBStateBackend} is used.
   *
   * <p>This affects the format of the generated savepoint, and should therefore be the same as what
   * is configured by the Stateful Functions application to be restored using the generated
   * savepoint.
   *
   * @return the savepoint creator, configured to use the {@link FsStateBackend}.
   */
  public StatefulFunctionsSavepointCreator withFsStateBackend() {
    this.stateBackend = new FsStateBackend("file:///tmp/ignored");
    return this;
  }

  /**
   * Registers a Flink {@link DataSet} to be used as inputs to {@link StateBootstrapFunction}s for
   * bootstrapping state. A provider for a {@link Router} that addresses each element in the
   * bootstrap dataset to {@link StateBootstrapFunction} instances must also be defined.
   *
   * <p>For all bootstrap functions that may receive a state bootstrap input, a {@link
   * StateBootstrapFunctionProvider} must also be registered for it using {@link
   * #withStateBootstrapFunctionProvider(FunctionType, StateBootstrapFunctionProvider)}.
   *
   * @param bootstrapDataset a Flink {@link DataSet} containing inputs for bootstrapping state.
   * @param routerProvider provider of a {@link Router} that addresses each element in the bootstrap
   *     dataset to {@link StateBootstrapFunction} instances.
   * @param <IN> data type of the input bootstrap dataset
   * @return the savepoint creator, configured to use the given bootstrap data
   */
  public <IN> StatefulFunctionsSavepointCreator withBootstrapData(
      DataSet<IN> bootstrapDataset, BootstrapDataRouterProvider<IN> routerProvider) {
    bootstrapDatasets.add(new BootstrapDataset<>(bootstrapDataset, routerProvider));
    return this;
  }

  /**
   * Registers a {@link StateBootstrapFunctionProvider} to the savepoint creator.
   *
   * @param functionType the type of function that is being bootstrapped.
   * @param bootstrapFunctionProvider the bootstrap function provider to register.
   * @return the savepoint creator, configured to use the given {@link
   *     StateBootstrapFunctionProvider}.
   */
  public StatefulFunctionsSavepointCreator withStateBootstrapFunctionProvider(
      FunctionType functionType, StateBootstrapFunctionProvider bootstrapFunctionProvider) {
    stateBootstrapFunctionRegistry.register(functionType, bootstrapFunctionProvider);
    return this;
  }

  /**
   * Writes the constructed savepoint to a given path.
   *
   * @param path path to write the generated savepoint to.
   */
  public void write(String path) {
    Preconditions.checkState(
        bootstrapDatasets.size() > 0, "At least 1 bootstrap DataSet must be registered.");
    Preconditions.checkState(
        stateBootstrapFunctionRegistry.numRegistrations() > 0,
        "At least 1 StateBootstrapFunctionProvider must be registered.");

    final NewSavepoint newSavepoint = Savepoint.create(stateBackend, maxParallelism);

    final DataSet<TaggedBootstrapData> taggedUnionBootstrapDataset =
        BootstrapDatasetUnion.apply(bootstrapDatasets);

    final BootstrapTransformation<TaggedBootstrapData> bootstrapTransformation =
        OperatorTransformation.bootstrapWith(taggedUnionBootstrapDataset)
            .keyBy(data -> data.getTarget().id())
            .transform(
                (timestamp, savepointPath) ->
                    new FunctionsStateBootstrapOperator(
                        stateBootstrapFunctionRegistry, timestamp, savepointPath));

    newSavepoint.withOperator(
        StatefulFunctionsJobConstants.FUNCTION_OPERATOR_UID, bootstrapTransformation);

    newSavepoint.write(path);
  }
}
