package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.BiFunction;

public class PartitionedMergeableValue<T> extends PersistedValue<T> implements PartitionedMergeableState {
    protected ArrayList<Accessor<T>> remotePartitionedAccessors;
    protected Accessor<T> mergedStateAccessor;
    protected Integer partitionId;
    protected Integer numPartitions;
    protected String mergedStateName;
    private BiFunction<T, T, T> mergingFunction;
    private final DataInputDeserializer inputView;
    private final DataOutputSerializer outputView;

    public PartitionedMergeableValue(String name, Class<T> type, Expiration expiration, Accessor<T> accessor, Boolean nftFlag, BiFunction<T, T, T> func, int partitionId, int numPartitions) {
        super(name, type, expiration, accessor, nftFlag);
        this.mergingFunction = func;
        this.mergedStateName = name;
        this.partitionId = partitionId;
        this.numPartitions = numPartitions;
        this.mergedStateAccessor = accessor;
        this.inputView = new DataInputDeserializer();
        this.outputView = new DataOutputSerializer(128);
    }

    public static <T> PartitionedMergeableValue<T> of(String name, Class<T> type, BiFunction<T, T, T> func, int partitionId, int numPartitions){
        return new PartitionedMergeableValue<T>(name, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions);
    }

    @Override
    public void setAllRemotePartitionedAccessors(ArrayList remoteAccessors) {
        remotePartitionedAccessors = (ArrayList<Accessor<T>>)remoteAccessors;
    }

    @Override
    public void mergeAllPartition() {
        T merged = remotePartitionedAccessors.get(0).get();
        for(int i = 1; i < remotePartitionedAccessors.size() ; i++){
            merged = mergingFunction.apply(merged, remotePartitionedAccessors.get(i).get());
        }
        accessor.set(merged);
    }

    @Override
    public Integer getNumPartitions() {
        return numPartitions;
    }

    @Override
    public void fromByteArray(byte[] array) {
        try {
            inputView.releaseArrays();
            inputView.setBuffer(array);
            T value = (T) getDescriptor().getSerializer().deserialize(inputView);
            T before = mergedStateAccessor.get();
            T after = mergingFunction.apply(before, value);
            mergedStateAccessor.set(after);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] toByteArray() {
        byte[] ret = null;
        try {
            T value = mergedStateAccessor.get();
            if(value != null && ((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive() && ((NonFaultTolerantAccessor) this.cachingAccessor).ifModified()){
                outputView.clear();
                getDescriptor().getSerializer().serialize(value, outputView);
                ret = outputView.getSharedBuffer();
                ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false);
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
        if(((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive()){
            if(((NonFaultTolerantAccessor) this.cachingAccessor).ifModified()) this.remotePartitionedAccessors.get(partitionId).set(this.cachingAccessor.get());
            ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false);
        }
    }
}

