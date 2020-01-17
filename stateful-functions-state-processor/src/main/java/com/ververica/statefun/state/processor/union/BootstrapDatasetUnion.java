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

package com.ververica.statefun.state.processor.union;

import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.io.Router;
import com.ververica.statefun.state.processor.BootstrapDataRouterProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/** Utility to union multiple {@link BootstrapDataset}s to a single, tagged Flink dataset. */
public final class BootstrapDatasetUnion {

  /**
   * Unions multiple {@link BootstrapDataset}s to a single, tagged Flink dataset.
   *
   * <p>This does a few things:
   *
   * <ul>
   *   <li>For each {@code BootstrapDataset}, assign a union index according to the order that they
   *       were given.
   *   <li>For each {@code BootstrapDataset}, tag each element with their assigned union index and
   *       target state bootstrap function address (as designated by routers). This is a flat map
   *       operation that transforms the original user-registered dataset to a dataset of {@link
   *       TaggedBootstrapData}.
   *   <li>Creates a type information that unions all type information of the data types of all
   *       user-registered datasets. The union serializer created by this type information multiplex
   *       multiple data types by pre-fixing each written record with their corresponding union
   *       index.
   * </ul>
   *
   * @param bootstrapDatasets pre-tagged, user-registered bootstrap datasets to union.
   * @return the union, tagged bootstrap dataset.
   */
  public static DataSet<TaggedBootstrapData> apply(List<BootstrapDataset<?>> bootstrapDatasets) {
    Objects.requireNonNull(bootstrapDatasets);
    Preconditions.checkArgument(bootstrapDatasets.size() > 0);

    final List<DataSet<TaggedBootstrapData>> unionBootstrapDataset =
        new ArrayList<>(bootstrapDatasets.size());

    final TypeInformation<TaggedBootstrapData> unionTypeInfo =
        createUnionTypeInfo(bootstrapDatasets);
    int unionIndex = 0;
    for (BootstrapDataset<?> bootstrapDataset : bootstrapDatasets) {
      unionBootstrapDataset.add(toTaggedFlinkDataSet(bootstrapDataset, unionIndex, unionTypeInfo));
      unionIndex++;
    }

    return unionTaggedBootstrapDataSets(unionBootstrapDataset);
  }

  private static TypeInformation<TaggedBootstrapData> createUnionTypeInfo(
      List<BootstrapDataset<?>> bootstrapDatasets) {
    List<TypeInformation<?>> payloadTypeInfos =
        bootstrapDatasets.stream()
            .map(bootstrapDataset -> bootstrapDataset.getDataSet().getType())
            .collect(Collectors.toList());

    return new TaggedBootstrapDataTypeInfo(payloadTypeInfos);
  }

  private static <T> DataSet<TaggedBootstrapData> toTaggedFlinkDataSet(
      BootstrapDataset<T> bootstrapDataset,
      int unionIndex,
      TypeInformation<TaggedBootstrapData> unionTypeInfo) {
    return bootstrapDataset
        .getDataSet()
        .flatMap(new BootstrapRouterFlatMap<>(bootstrapDataset.getRouterProvider(), unionIndex))
        .returns(unionTypeInfo);
  }

  private static DataSet<TaggedBootstrapData> unionTaggedBootstrapDataSets(
      List<DataSet<TaggedBootstrapData>> taggedBootstrapDatasets) {
    DataSet<TaggedBootstrapData> result = null;
    for (DataSet<TaggedBootstrapData> taggedBootstrapDataDataset : taggedBootstrapDatasets) {
      if (result != null) {
        result = result.union(taggedBootstrapDataDataset);
      } else {
        result = taggedBootstrapDataDataset;
      }
    }

    return result;
  }

  private static class BootstrapRouterFlatMap<T>
      extends RichFlatMapFunction<T, TaggedBootstrapData> {

    private static final long serialVersionUID = 1L;

    private final BootstrapDataRouterProvider<T> routerProvider;
    private final int unionIndex;

    private transient Router<T> router;

    BootstrapRouterFlatMap(BootstrapDataRouterProvider<T> routerProvider, int unionIndex) {
      this.routerProvider = Objects.requireNonNull(routerProvider);

      Preconditions.checkArgument(unionIndex >= 0);
      this.unionIndex = unionIndex;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      this.router = routerProvider.provide();
    }

    @Override
    public void flatMap(T data, Collector<TaggedBootstrapData> collector) throws Exception {
      router.route(data, new TaggingBootstrapDataCollector<>(collector, unionIndex));
    }
  }

  /**
   * A collector which tags each collected element with their union index and designated address.
   */
  private static class TaggingBootstrapDataCollector<T> implements Router.Downstream<T> {
    private final Collector<TaggedBootstrapData> out;
    private final int unionIndex;

    TaggingBootstrapDataCollector(Collector<TaggedBootstrapData> out, int unionIndex) {
      this.out = Objects.requireNonNull(out);
      this.unionIndex = unionIndex;
    }

    @Override
    public void forward(FunctionType functionType, String id, T message) {
      out.collect(new TaggedBootstrapData(new Address(functionType, id), message, unionIndex));
    }

    @Override
    public void forward(Address to, T message) {
      out.collect(new TaggedBootstrapData(to, message, unionIndex));
    }
  }
}
