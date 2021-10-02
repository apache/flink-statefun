package org.apache.flink.statefun.flink.core.functions.utils;

import org.apache.flink.statefun.flink.core.common.KeyBy;
import org.apache.flink.statefun.sdk.Address;

import java.util.HashMap;

public class MessageCounter {
    private final HashMap<String, Long> partitionToMessageCount;

    public MessageCounter(){
        partitionToMessageCount = new HashMap<>();
    }

    public Long increment(Address to){
        String targetPartition = KeyBy.apply(to);
        Long count = 0L;
        if(partitionToMessageCount.containsKey(targetPartition)){
            count = partitionToMessageCount.get(targetPartition);
            partitionToMessageCount.put(targetPartition, ++count);
        }
        else{
            partitionToMessageCount.put(targetPartition, 1L);
        }
        return count;
    }
}
