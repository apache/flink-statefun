package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sdk.state.StateAccessDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BiFunction;

public class PartitionedMergeableValue<T> extends PersistedValue<T> implements PartitionedMergeableState {
    protected ArrayList<Accessor<T>> remotePartitionedAccessors;
    protected Accessor<T> mergedStateAccessor;
    protected Integer partitionId;
    protected Integer numPartitions;
    private BiFunction<T, T, T> mergingFunction;
    private final DataInputDeserializer inputView;
    private final DataOutputSerializer outputView;
    private final StateAccessDescriptor stateAccessDescriptor;

    public PartitionedMergeableValue(String name,
                                     StateAccessDescriptor descriptor,
                                     Class<T> type,
                                     Expiration expiration,
                                     Accessor<T> accessor, Boolean nftFlag, BiFunction<T, T, T> func,
                                     int partitionId,
                                     int numPartitions,
                                     Mode accessMode
    ) {
        super(name, type, expiration, accessor, nftFlag);
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

    public static <T> PartitionedMergeableValue<T> of(String name, StateAccessDescriptor descriptor, Class<T> type, BiFunction<T, T, T> func, int partitionId, int numPartitions){
        return new PartitionedMergeableValue<T>(name, descriptor, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, Mode.EXCLUSIVE);
    }

    public static <T> PartitionedMergeableValue<T> of(String name, StateAccessDescriptor descriptor, Class<T> type, BiFunction<T, T, T> func, int partitionId, int numPartitions, Mode accessMode){
        return new PartitionedMergeableValue<T>(name, descriptor, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, accessMode);
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
            System.out.println("Deserialize " + name() + " to object " + (value==null?"null":value) + " tid: " + Thread.currentThread().getName());
            T before = mergedStateAccessor.get();
            System.out.println("Merge before " + name() + " to " + (before == null? "null":before) + " tid: " + Thread.currentThread().getName());
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
            System.out.println("Try to serialize value " + (value==null?"null":value) + " tid: " + Thread.currentThread().getName());
            if(value != null && ((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive() && ((NonFaultTolerantAccessor) this.cachingAccessor).ifModified()){
                outputView.clear();
                getDescriptor().getSerializer().serialize(value, outputView);
                System.out.println("Serialize value " + value + " tid: " + Thread.currentThread().getName());
                ret = outputView.getSharedBuffer();
                ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false);
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
        if(((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive()){
            if(((NonFaultTolerantAccessor) this.cachingAccessor).ifModified()) this.remotePartitionedAccessors.get(partitionId).set(this.cachingAccessor.get());
            ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "PartitionedMergeableValue{name=%s, type=%s, expiration=%s, lessor=<%s>, owners=<%s>, mode=<%s>}",
                name, type.getName(), expiration, lessor, Arrays.toString(accessors.toArray()), getMode().toString());
    }
}

