package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.*;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;

public class PartitionedMergeableTable<K, V> extends PersistedTable<K, V> implements PartitionedMergeableState {
    protected ArrayList<TableAccessor<K, V>> remotePartitionedAccessors;
    protected TableAccessor<K, V> mergedStateAccessor;
    protected Integer numPartitions;
    private Integer partitionId;
    private BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> mergingFunction;
    private final DataInputDeserializer inputView;
    private final DataOutputSerializer outputView;
    private final StateAccessDescriptor stateAccessDescriptor;

    public PartitionedMergeableTable(String name,
                                     StateAccessDescriptor descriptor,
                                     Class<K> keyType,
                                     Class<V> valueType,
                                     Expiration expiration,
                                     TableAccessor<K, V> accessor,
                                     Boolean nftFlag,
                                     BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> func,
                                     int partitionId,
                                     int numPartitions,
                                     Mode accessMode
    ) {
        super(name, keyType, valueType, expiration, accessor, nftFlag);
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

    public static <K, V> PartitionedMergeableTable<K, V> of(String name, StateAccessDescriptor descriptor, Class<K> keyType, Class<V> valueType, BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> func, int partitionId, int numPartitions){
        return new PartitionedMergeableTable<>(name, descriptor, keyType, valueType, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, Mode.EXCLUSIVE);
    }

    public static <K, V> PartitionedMergeableTable<K, V> of(String name, StateAccessDescriptor descriptor, Class<K> keyType, Class<V> valueType, BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> func, int partitionId, int numPartitions, Mode accessMode){
        return new PartitionedMergeableTable<>(name, descriptor, keyType, valueType, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func, partitionId, numPartitions, accessMode);
    }

    @Override
    public void setAllRemotePartitionedAccessors(ArrayList remoteAccessors) {
        remotePartitionedAccessors = (ArrayList<TableAccessor<K, V>>)remoteAccessors;
    }

    @Override
    public void mergeAllPartition() {
        Iterable<Map.Entry<K, V>> merged = remotePartitionedAccessors.get(0).entries();
        for(int i = 1; i < remotePartitionedAccessors.size(); i++){
            merged = mergingFunction.apply(merged, remotePartitionedAccessors.get(i).entries());
        }
        accessor.clear();
        for(Map.Entry<K, V> entry : merged){
            accessor.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Integer getNumPartitions() {
        return numPartitions;
    }

    @Override
    public void fromByteArray(byte[] array) {
        try {
            //MapSerializer
            inputView.releaseArrays();
            inputView.setBuffer(array);
            MapStateDescriptor<K, V> descriptor = (MapStateDescriptor<K, V>) getDescriptor();
            Iterable<Map.Entry<K, V>> deserailizedResult = descriptor.getSerializer().deserialize(inputView).entrySet();
            Iterable<Map.Entry<K, V>> before = mergedStateAccessor.entries();
            Iterable<Map.Entry<K, V>> after  = mergingFunction.apply(before, deserailizedResult);
            mergedStateAccessor.clear();
            for(Map.Entry<K, V> entry : after){
                mergedStateAccessor.set(entry.getKey(), entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] toByteArray() {
        byte[] ret = null;
        try {
        MapStateDescriptor<K, V> descriptor = (MapStateDescriptor<K, V>) getDescriptor();
        Map<K, V> value = new HashMap<>();
        mergedStateAccessor.entries().forEach(
            kv->value.put(kv.getKey(), kv.getValue())
        );
        if(!value.isEmpty() && this.cachingAccessor.ifActive() && this.cachingAccessor.ifModified()){
            outputView.clear();
            descriptor.getSerializer().serialize(value, outputView);
            ret = outputView.getSharedBuffer();
            this.cachingAccessor.setActive(false);
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
            if(this.cachingAccessor.ifModified()) {
                this.remotePartitionedAccessors.get(partitionId).clear();
                for(Map.Entry<K, V> pair : this.cachingAccessor.entries()){
                    this.remotePartitionedAccessors.get(partitionId).set(pair.getKey(), pair.getValue());
                }
            }
            this.cachingAccessor.setActive(false);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "PartitionedMergeableTable{name=%s, keyType=%s, valueType=%s, expiration=%s, lessor=<%s>, owners=<%s>}",
                name, keyType.getName(), valueType.getName(), expiration, lessor, Arrays.toString(accessors.toArray()));
    }
}
