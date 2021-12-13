package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class PartitionedMergeableAppendingBuffer<T> extends PersistedAppendingBuffer<T> implements PartitionedMergeableState {
    protected ArrayList<AppendingBufferAccessor<T>> remotePartitionedAccessors;
    protected AppendingBufferAccessor<T> mergedStateAccessor;
    protected Integer partitionId;
    protected Integer numPartitions;
    protected String mergedStateName;
    private BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> mergingFunction;
    private final DataInputDeserializer inputView;
    private final DataOutputSerializer outputView;

    private PartitionedMergeableAppendingBuffer(
            String name,
            Class<T> elementType,
            Expiration expiration,
            AppendingBufferAccessor<T> accessor,
            Boolean nftFlag,
            BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func,
            int partitionId,
            int numPartitions) {
        super(name, elementType, expiration, accessor, nftFlag);
        this.mergingFunction = func;
        this.mergedStateName = name;
        this.partitionId = partitionId;
        this.numPartitions = numPartitions;
        this.mergedStateAccessor = accessor;
        this.inputView = new DataInputDeserializer();
        this.outputView = new DataOutputSerializer(128);
    }

    public static <T> PersistedAppendingBuffer<T> of(String name, Class<T> elementType, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func, int partitionId, int numPartitions) {
        return new PartitionedMergeableAppendingBuffer<>(name, elementType, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions);
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
}
