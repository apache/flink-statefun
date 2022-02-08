package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.Partition;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class RandomLesseeSelector extends LesseeSelector {

    private int EXPLORE_RANGE = 1;

    private transient static final Logger LOG = LoggerFactory.getLogger(RandomLesseeSelector.class);
    private List<Integer> targetIdList;

    public RandomLesseeSelector(Partition partition) {
        this.partition = partition;
        this.targetIdList = new ArrayList<>();
        for (int i = 0; i < partition.getParallelism(); i++){
//            if(i != partition.getThisOperatorIndex()){
                this.targetIdList.add(i);
//            }
        }
        LOG.debug("Initialize QueueBasedLesseeSelector operator index {} parallelism {} max parallelism {} keygroup {}",
                partition.getThisOperatorIndex(), partition.getParallelism(), partition.getMaxParallelism(),
                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), partition.getThisOperatorIndex()));
    }

    public RandomLesseeSelector(Partition partition, int range) {
        this.EXPLORE_RANGE = range;
        this.partition = partition;
        this.targetIdList = new ArrayList<>();
        for (int i = 0; i < partition.getParallelism(); i++){
//            if(i != partition.getThisOperatorIndex()){
                this.targetIdList.add(i);
//            }
        }
	LOG.debug("Initialize RandomLesseeSelector operator index {} parallelism {} max parallelism {} keygroup {} EXPLORE_RANGE {}",
                partition.getThisOperatorIndex(), partition.getParallelism(), partition.getMaxParallelism(),
                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), partition.getThisOperatorIndex()),
                EXPLORE_RANGE);
    }

    @Override
    public Address selectLessee(Address lessorAddress) {
        int targetOperatorId = ThreadLocalRandom.current().nextInt(0, partition.getParallelism());
        //int targetOperatorId = ((ThreadLocalRandom.current().nextInt()%(partition.getParallelism()-1) + (partition.getParallelism()-1))%(partition.getParallelism()-1) + partition.getThisOperatorIndex() + 1)%(partition.getParallelism());
        System.out.println("targetOperatorId " + targetOperatorId + " partition.getThisOperatorIndex() " + partition.getThisOperatorIndex());
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
        return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
    }

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
            return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
        }).collect(Collectors.toSet());
    }

    @Override
    public ArrayList<Address> exploreLessee(Address address) {
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < this.EXPLORE_RANGE; i++){
            int targetOperatorIdIndex =(int)(ThreadLocalRandom.current().nextDouble() * (partition.getParallelism()));
            int targetOperatorId = this.targetIdList.get(targetOperatorIdIndex);
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
        }
        return ret;
    }

    @Override
    public ArrayList<Address> exploreLesseeWithBase(Address address) {
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < this.EXPLORE_RANGE; i++){
            int targetOperatorIdIndex =(int)(ThreadLocalRandom.current().nextDouble() * (partition.getParallelism()));
            int targetOperatorId = this.targetIdList.get(targetOperatorIdIndex);
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
        }
        return ret;
    }
}
