package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.*;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

public class PartitionedMergeableList<T> extends PersistedList<T> implements PartitionedMergeableState {
    protected ArrayList<ListAccessor<T>> remotePartitionedAccessors;
    protected ListAccessor<T> mergedStateAccessor;
    protected Integer numPartitions;
    private Integer partitionId;
    private BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> mergingFunction;
    private final DataInputDeserializer inputView;
    private final DataOutputSerializer outputView;
    private final StateAccessDescriptor stateAccessDescriptor;

    private PartitionedMergeableList(String name,
                                     StateAccessDescriptor descriptor,
                                     Class type,
                                     Expiration expiration,
                                     ListAccessor<T> accessor,
                                     Boolean nftFlag,
                                     BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func,
                                     int partitionId,
                                     int numPartitions,
                                     Mode accessMode
    ) {
        super(name, type, expiration, accessor, nftFlag);
        this.stateAccessDescriptor = descriptor;
        this.mergingFunction = func;
        this.numPartitions = numPartitions;
        this.partitionId = partitionId;
        this.mergedStateAccessor = accessor;
        this.inputView = new DataInputDeserializer();
        this.outputView = new DataOutputSerializer(128);
        this.lessor = descriptor.getLessor();
        this.accessors.add(descriptor.getCurrentAccessor());
        this.setMode(accessMode);
    }

    public static <T> PartitionedMergeableList<T> of(String name, StateAccessDescriptor descriptor, Class<T> type, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func, int partitionId, int numPartitions) {
        return new PartitionedMergeableList<>(name, descriptor, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, Mode.EXCLUSIVE);
    }

    public static <T> PartitionedMergeableList<T> of(String name, StateAccessDescriptor descriptor, Class<T> type, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func, int partitionId, int numPartitions, Mode accessMode){
        return new PartitionedMergeableList<>(name, descriptor, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, accessMode);
    }

    @Override
    public void setAllRemotePartitionedAccessors(ArrayList remoteAccessors) {
        remotePartitionedAccessors = (ArrayList<ListAccessor<T>>)remoteAccessors;
    }

    @Override
    public void mergeAllPartition() {
        Iterable<T> merged = remotePartitionedAccessors.get(0).get();
        for(int i = 1; i < remotePartitionedAccessors.size() ; i++){
            merged = mergingFunction.apply(merged, remotePartitionedAccessors.get(i).get());
        }
        accessor.update((List<T>) merged);
    }

    @Override
    public Integer getNumPartitions() {
        return numPartitions;
    }

    @Override
    public void fromByteArray(byte[] array) {
        inputView.releaseArrays();
        inputView.setBuffer(array);
        ListStateDescriptor<T> descriptor = (ListStateDescriptor<T>)getDescriptor();
        try {
            List<T> deserializedResult = descriptor.getSerializer().deserialize(inputView);
            List<T> before = (List<T>) mergedStateAccessor.get();
            List<T> after = (List<T>) mergingFunction.apply(before, deserializedResult);
            mergedStateAccessor.update(after);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static <T> T deserializeNextElement(
            DataInputDeserializer in,
            TypeSerializer<T> elementSerializer) {
        try {
            if (in.available() > 0) {
                T element = elementSerializer.deserialize(in);
                if (in.available() > 0) {
                    in.readByte();
                }
                return element;
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("Unexpected list element deserialization failure", e);
        }
        return null;
    }

    @Override
    public byte[] toByteArray() {
        byte[] ret = null;
        try {
            ListStateDescriptor<T> descriptor = (ListStateDescriptor<T>)getDescriptor();
            List<T> values = (List<T>) mergedStateAccessor.get();
            if(values != null && !values.isEmpty() && this.cachingAccessor.ifActive() && this.cachingAccessor.ifModified()){
                outputView.clear();
                descriptor.getSerializer().serialize(values, outputView);
                ret = outputView.getSharedBuffer();
            }
            this.cachingAccessor.setActive(false);
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
            if(this.cachingAccessor.ifModified()) this.remotePartitionedAccessors.get(partitionId).update((List<T>) this.cachingAccessor.get());
            this.cachingAccessor.setActive(false);
        }
    }

    @Override
    public String toString(){
        return String.format(
                "PartitionedMergeableList{name=%s, elementType=%s, expiration=%s, lessor=<%s>, owners=<%s>, mode=<%s>}",
                name, elementType.getName(), expiration, lessor, Arrays.toString(accessors.toArray()), getMode().toString());
    }
}
