package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.util.ArrayList;
import java.util.function.BiFunction;

public class PartitionedMergeableValueState<T> extends PersistedValue<T> implements PartitionedMergeableState {
    private BiFunction<T, T, T> mergingFunction;
    protected ArrayList<Accessor<T>> remotePartitionedAccessors;
    protected Accessor<T> mergedStateAccessor;
    protected Integer partitionId;
    protected Integer numPartitions;
    protected String mergedStateName;

    public PartitionedMergeableValueState(String name, Class<T> type, Expiration expiration, Accessor<T> accessor, Boolean nftFlag, BiFunction<T, T, T> func, int partitionId, int numPartitions) {
        super(name, type, expiration, accessor, nftFlag);
        this.mergingFunction = func;
        this.mergedStateName = name;
        this.partitionId = partitionId;
        this.numPartitions = numPartitions;
        this.mergedStateAccessor = accessor;
    }

    public static <T> PartitionedMergeableValueState<T> of(String name, Class<T> type, BiFunction<T, T, T> func, int partitionId, int numPartitions){
        return new PartitionedMergeableValueState<T>(name, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions);
    }

    @Override
    public void setAllRemotePartitionedAccessors(ArrayList remoteAccessors) {
        remotePartitionedAccessors = (ArrayList<Accessor<T>>)remoteAccessors;
        //setAccessor(remotePartitionedAccessors.get(partitionId));
    }

    @Override
    public void mergeAllPartition() {
        T merged = remotePartitionedAccessors.get(0).get();
        for(int i = 1; i < remotePartitionedAccessors.size() ; i++){
            System.out.println("Merging to remote accessor " + remotePartitionedAccessors.get(i) + " tid " + Thread.currentThread().getName());
            merged = mergingFunction.apply(merged, remotePartitionedAccessors.get(i).get());
        }
        accessor.set(merged);
    }

    @Override
    public Integer getNumPartitions() {
        return numPartitions;
    }

    @Override
    public boolean ifPartitioned(){
        return true;
    }

    @Override
    public void flush() {
        System.out.println("Flush to remote accessor " + accessor + " tid " + Thread.currentThread().getName() + " nft accessor active " + ((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive());
        if(((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive()){
            System.out.println("" +
                    " "+ this.cachingAccessor.get() + " to " + accessor + " tid " + Thread.currentThread().getName() + " partition id " + partitionId);
            if(((NonFaultTolerantAccessor) this.cachingAccessor).ifModified()) this.remotePartitionedAccessors.get(partitionId).set(this.cachingAccessor.get());
            ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false);
        }
    }
}

