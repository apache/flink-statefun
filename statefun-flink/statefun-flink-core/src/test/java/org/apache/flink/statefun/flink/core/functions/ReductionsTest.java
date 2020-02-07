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
package org.apache.flink.statefun.flink.core.functions;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.BiConsumerWithException;
import org.junit.Test;

public class ReductionsTest {

  @Test
  public void testFactory() {
    Reductions reductions =
        Reductions.create(
            new StatefulFunctionsUniverse(MessageFactoryType.WITH_KRYO_PAYLOADS),
            new FakeRuntimeContext(),
            new FakeKeyedStateBackend(),
            new FakeTimerServiceFactory(),
            new FakeInternalListState(),
            new HashMap<>(),
            new FakeOutput(),
            TestUtils.ENVELOPE_FACTORY,
            MoreExecutors.directExecutor(),
            new FakeMetricGroup(),
            new FakeMapState(),
            MoreExecutors.directExecutor());

    assertThat(reductions, notNullValue());
  }

  @SuppressWarnings("deprecation")
  private static final class FakeRuntimeContext implements RuntimeContext {

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
      return new ValueState<T>() {
        @Override
        public T value() {
          return null;
        }

        @Override
        public void update(T value) {}

        @Override
        public void clear() {}
      };
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
      return new MapState<UK, UV>() {
        @Override
        public UV get(UK key) {
          return null;
        }

        @Override
        public void put(UK key, UV value) {}

        @Override
        public void putAll(Map<UK, UV> map) {}

        @Override
        public void remove(UK key) {}

        @Override
        public boolean contains(UK key) {
          return false;
        }

        @Override
        public Iterable<Entry<UK, UV>> entries() {
          return null;
        }

        @Override
        public Iterable<UK> keys() {
          return null;
        }

        @Override
        public Iterable<UV> values() {
          return null;
        }

        @Override
        public Iterator<Entry<UK, UV>> iterator() {
          return null;
        }

        @Override
        public boolean isEmpty() throws Exception {
          return true;
        }

        @Override
        public void clear() {}
      };
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
      return new ExecutionConfig();
    }

    // everything below this line would throw UnspportedOperationException()

    @Override
    public String getTaskName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricGroup getMetricGroup() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
      return 0;
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
      return 0;
    }

    @Override
    public int getIndexOfThisSubtask() {
      return 0;
    }

    @Override
    public int getAttemptNumber() {
      return 0;
    }

    @Override
    public String getTaskNameWithSubtasks() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <V, A extends Serializable> void addAccumulator(
        String name, Accumulator<V, A> accumulator) {}

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Accumulator<?, ?>> getAllAccumulators() {
      throw new UnsupportedOperationException();
    }

    @Override
    public IntCounter getIntCounter(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public LongCounter getLongCounter(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Histogram getHistogram(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasBroadcastVariable(String name) {
      return false;
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(
        String name, BroadcastVariableInitializer<T, C> initializer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DistributedCache getDistributedCache() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
        AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T, ACC> FoldingState<T, ACC> getFoldingState(
        FoldingStateDescriptor<T, ACC> stateProperties) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FakeKeyedStateBackend implements KeyedStateBackend<Object> {

    @Override
    public <N, S extends State, T> void applyToAllKeys(
        N namespace,
        TypeSerializer<N> namespaceSerializer,
        StateDescriptor<S, T> stateDescriptor,
        KeyedStateFunction<Object, S> function) {}

    @Override
    public <N> Stream<Object> getKeys(String state, N namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <N, S extends State, T> S getOrCreateKeyedState(
        TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <N, S extends State> S getPartitionedState(
        N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dispose() {}

    @Override
    public void registerKeySelectionListener(KeySelectionListener<Object> listener) {}

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<Object> listener) {
      return false;
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
        @Nonnull TypeSerializer<N> namespaceSerializer,
        @Nonnull StateDescriptor<S, SV> stateDesc,
        @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
      throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed>
        KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getCurrentKey() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setCurrentKey(Object newKey) {}

    @Override
    public TypeSerializer<Object> getKeySerializer() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FakeTimerServiceFactory implements TimerServiceFactory {

    @Override
    public InternalTimerService<VoidNamespace> createTimerService(
        Triggerable<String, VoidNamespace> triggerable) {
      return new FakeTimerService();
    }
  }

  private static final class FakeTimerService implements InternalTimerService<VoidNamespace> {

    @Override
    public long currentProcessingTime() {
      return 0;
    }

    @Override
    public long currentWatermark() {
      return 0;
    }

    @Override
    public void registerEventTimeTimer(VoidNamespace namespace, long time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerProcessingTimeTimer(VoidNamespace namespace, long time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEventTimeTimer(VoidNamespace namespace, long time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteProcessingTimeTimer(VoidNamespace namespace, long time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void forEachEventTimeTimer(
        BiConsumerWithException<VoidNamespace, Long, Exception> consumer) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void forEachProcessingTimeTimer(
        BiConsumerWithException<VoidNamespace, Long, Exception> consumer) throws Exception {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FakeInternalListState
      implements InternalListState<String, Long, Message> {

    @Override
    public void add(Message value) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(List<Message> values) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void update(List<Message> values) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateInternal(List<Message> valueToStore) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setCurrentNamespace(Long namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getSerializedValue(
        byte[] serializedKeyAndNamespace,
        TypeSerializer<String> safeKeySerializer,
        TypeSerializer<Long> safeNamespaceSerializer,
        TypeSerializer<List<Message>> safeValueSerializer)
        throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Message> getInternal() throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Message> get() throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void mergeNamespaces(Long target, Collection<Long> sources) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public StateIncrementalVisitor<String, Long, List<Message>> getStateIncrementalVisitor(
        int recommendedMaxNumberOfReturnedRecords) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer<Long> getNamespaceSerializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer<String> getKeySerializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer<List<Message>> getValueSerializer() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FakeMapState implements MapState<Long, Message> {

    @Override
    public Message get(Long key) throws Exception {
      return null;
    }

    @Override
    public void put(Long key, Message value) throws Exception {}

    @Override
    public void putAll(Map<Long, Message> map) throws Exception {}

    @Override
    public void remove(Long key) throws Exception {}

    @Override
    public boolean contains(Long key) throws Exception {
      return false;
    }

    @Override
    public Iterable<Entry<Long, Message>> entries() throws Exception {
      return null;
    }

    @Override
    public Iterable<Long> keys() throws Exception {
      return null;
    }

    @Override
    public Iterable<Message> values() throws Exception {
      return null;
    }

    @Override
    public Iterator<Entry<Long, Message>> iterator() throws Exception {
      return null;
    }

    @Override
    public boolean isEmpty() throws Exception {
      return true;
    }

    @Override
    public void clear() {}
  }

  private static final class FakeOutput implements Output<StreamRecord<Message>> {

    @Override
    public void emitWatermark(Watermark mark) {}

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {}

    @Override
    public void collect(StreamRecord<Message> record) {}

    @Override
    public void close() {}
  }

  private static final class FakeMetricGroup implements MetricGroup {
    @Override
    public Counter counter(int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Counter counter(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <C extends Counter> C counter(int i, C c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <C extends Counter> C counter(String s, C c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(int i, G g) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String s, G g) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <H extends org.apache.flink.metrics.Histogram> H histogram(String s, H h) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <H extends org.apache.flink.metrics.Histogram> H histogram(int i, H h) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <M extends Meter> M meter(String s, M m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <M extends Meter> M meter(int i, M m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricGroup addGroup(int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricGroup addGroup(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricGroup addGroup(String s, String s1) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getScopeComponents() {
      return new String[0];
    }

    @Override
    public Map<String, String> getAllVariables() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getMetricIdentifier(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getMetricIdentifier(String s, CharacterFilter characterFilter) {
      throw new UnsupportedOperationException();
    }
  }
}
