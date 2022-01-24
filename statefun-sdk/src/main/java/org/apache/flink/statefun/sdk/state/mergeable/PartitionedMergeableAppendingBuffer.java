package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

public class PartitionedMergeableAppendingBuffer<T> extends PersistedAppendingBuffer<T> implements PartitionedMergeableState {
    protected ArrayList<AppendingBufferAccessor<T>> remotePartitionedAccessors;
    protected AppendingBufferAccessor<T> mergedStateAccessor;
    protected Integer partitionId;
    protected Integer numPartitions;
    private BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> mergingFunction;
    private final DataInputDeserializer inputView;
    private final DataOutputSerializer outputView;
    private final StateAccessDescriptor stateAccessDescriptor;

    private PartitionedMergeableAppendingBuffer(
            String name,
            StateAccessDescriptor descriptor,
            Class<T> elementType,
            Expiration expiration,
            AppendingBufferAccessor<T> accessor,
            Boolean nftFlag,
            BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func,
            int partitionId,
            int numPartitions,
            Mode accessMode
    ) {
        super(name, elementType, expiration, accessor, nftFlag);
        this.stateAccessDescriptor = descriptor;
        this.mergingFunction = func;
        this.partitionId = partitionId;
        this.numPartitions = numPartitions;
        this.mergedStateAccessor = accessor;
        this.inputView = new DataInputDeserializer();
        this.outputView = new DataOutputSerializer(128);
        this.lessor = descriptor.getLessor();
        this.accessors.add(descriptor.getCurrentAccessor());
        this.setMode(accessMode);
    }

    public static <T> PersistedAppendingBuffer<T> of(String name, StateAccessDescriptor descriptor, Class<T> elementType, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func, int partitionId, int numPartitions) {
        return new PartitionedMergeableAppendingBuffer<>(name, descriptor, elementType, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, Mode.EXCLUSIVE);
    }

    public static <T> PersistedAppendingBuffer<T> of(String name, StateAccessDescriptor descriptor, Class<T> elementType, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func, int partitionId, int numPartitions, Mode accessMode) {
        return new PartitionedMergeableAppendingBuffer<>(name, descriptor, elementType, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, accessMode);
    }

    @Override
    public void setAllRemotePartitionedAccessors(ArrayList remoteAccessors) {
        remotePartitionedAccessors = (ArrayList<AppendingBufferAccessor<T>>)remoteAccessors;
    }

    @Override
    public void mergeAllPartition() {
        Iterable<T> merged = remotePartitionedAccessors.get(0).view();
        for(int i = 1; i < remotePartitionedAccessors.size() ; i++){
            merged = mergingFunction.apply(merged, remotePartitionedAccessors.get(i).view());
        }
        accessor.replaceWith((List<T>) merged);
    }

    @Override
    public Integer getNumPartitions() {
        return numPartitions;
    }

    @Override
    public void fromByteArray(byte[] array) {
        inputView.releaseArrays();
        inputView.setBuffer(array);
        try {
            ListStateDescriptor<T> descriptor = (ListStateDescriptor<T>)getDescriptor();
            List<T> value = descriptor.getSerializer().deserialize(inputView);
            List<T> before = (List<T>) mergedStateAccessor.view();
            List<T> after = new ArrayList<>();
            mergingFunction.apply(before, value).forEach(after::add);
            mergedStateAccessor.replaceWith(after);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public byte[] toByteArray() {
        byte[] ret = null;
        try {
            Iterable<T> valueIterables = mergedStateAccessor.view();
            if(this.cachingAccessor.ifActive() && this.cachingAccessor.ifModified()){
                ListStateDescriptor<T> descriptor = (ListStateDescriptor<T>)getDescriptor();
                outputView.clear();
                List<T> values = new ArrayList<>();
                valueIterables.forEach(values::add);
                descriptor.getSerializer().serialize(values, outputView);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public StateAccessDescriptor getStateAccessDescriptor() {
        return stateAccessDescriptor;
    }

    @Override
    public boolean ifPartitioned(){
        return true;
    }

    @Override
    public void flush() {
        if(this.cachingAccessor.ifActive()){
            if(this.cachingAccessor.ifModified()) this.remotePartitionedAccessors.get(partitionId).replaceWith((List<T>) this.cachingAccessor.view());
            this.cachingAccessor.setActive(false);
        }
    }

    @Override
    public String toString(){
        return String.format(
                "PartitionedMergeableAppendingBuffer{name=%s, elementType=%s, expiration=%s, lessor=<%s>, owners=<%s>, mode=<%s>}",
                name, elementType.getName(), expiration, lessor, Arrays.toString(accessors.toArray()), getMode().toString());
    }
}
