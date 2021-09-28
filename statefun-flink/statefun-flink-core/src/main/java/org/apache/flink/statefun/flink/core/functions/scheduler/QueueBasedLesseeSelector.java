package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.Partition;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class QueueBasedLesseeSelector extends LesseeSelector {
    private  int EXPLORE_RANGE = 1;

    private transient static final Logger LOG = LoggerFactory.getLogger(QueueBasedLesseeSelector.class);
    private HashMap<Integer, Integer> history;
    private ReusableContext context;

    public QueueBasedLesseeSelector(Partition partition, ReusableContext context){
        this.history = new HashMap<>();
        this.partition = partition;
        this.context = context;
        LOG.debug("Initialize QueueBasedLesseeSelector operator index {} parallelism {} max parallelism {} keygroup {}",
                partition.getThisOperatorIndex(), partition.getParallelism(), partition.getMaxParallelism(),
                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), partition.getThisOperatorIndex()));
    }

    @Override
    public Address selectLessee(Address lessorAddress) {
        if(history.isEmpty()){
            int targetOperatorId = ((ThreadLocalRandom.current().nextInt()%(partition.getParallelism()-1) + (partition.getParallelism()-1))%(partition.getParallelism()-1) + partition.getThisOperatorIndex() + 1)%(partition.getParallelism());
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
        }
        int targetOperatorId = history.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).findFirst().get().getKey();
//        LOG.debug(" sorted history " + history.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).map(kv->kv.getKey() + ": " + kv.getValue()).collect(Collectors.joining("|"))
//        + " targetOperatorId " + targetOperatorId);
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
        return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
    }

    //TODO: queue based?
    @Override
    public Set<Address> selectLessees(Address lessorAddress, int count) {
        if(count >= partition.getParallelism()) throw new FlinkRuntimeException("Cannot select number of lessees greater than parallelism level.");
        HashSet<Integer> targetIds = new HashSet<>();
        for(int i = 0; i < count; i++){
            int targetOperatorId = ((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
            while(targetIds.contains(targetOperatorId)){
                targetOperatorId = ((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
            }
            targetIds.add(targetOperatorId);
        }
        return targetIds.stream().map(targetOperatorId->{
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            return new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId));
        }).collect(Collectors.toSet());
    }

    @Override
    public void collect(Address address, Integer queueSize) {
        int keyGroupId = Integer.parseInt(address.id());
        history.put(keyGroupId, queueSize);
    }

    @Override
    public ArrayList<Address> exploreLessee() {
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < EXPLORE_RANGE; i++){
            int targetOperatorId = ((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)));
        }
        return ret;
    }

    @Override
    public ArrayList<Address> exploreLesseeWithBase(Address address) {
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < EXPLORE_RANGE; i++){
            int targetOperatorId = ((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
        }
        return ret;
    }
}
