package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.common.KeyBy;
import org.apache.flink.statefun.flink.core.functions.Partition;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator;

public class RandomIdSpanLesseeSelector extends SpanLesseeSelector {
    private int EXPLORE_RANGE = 1;
    private int MAX_SPAN = 4;

    private transient static final Logger LOG = LoggerFactory.getLogger(RandomIdSpanLesseeSelector.class);
    private final List<Integer> targetIdList;
    private final List<Integer> targetIdListExcludingSelf;

    public RandomIdSpanLesseeSelector(Partition partition, int range, int searchSpan) {
        this.EXPLORE_RANGE = range;
        this.MAX_SPAN = searchSpan;
        this.partition = partition;
        this.targetIdList = new ArrayList<>();
        this.targetIdListExcludingSelf = new ArrayList<>();
        for (int i = 0; i < MAX_SPAN; i++){
            this.targetIdList.add(i);
        }
        for (int i = 1; i < MAX_SPAN; i++){
            this.targetIdListExcludingSelf.add(i);
        }
        if(EXPLORE_RANGE > MAX_SPAN){
            throw new FlinkRuntimeException(String.format("EXPLORE_RANGE %d cannot be greater MAX_SPAN %d \n", EXPLORE_RANGE, MAX_SPAN));
        }
        LOG.info("Initialize RandomIdSpanLesseeSelector operator index {} parallelism {} max parallelism {} keygroup id list {} EXPLORE_RANGE {} MAX_SPAN {} targetIdList {} targetIdListExcludingSelf {}",
                partition.getThisOperatorIndex(), partition.getParallelism(), partition.getMaxParallelism(),
                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(),partition.getThisOperatorIndex()),
                        EXPLORE_RANGE, MAX_SPAN, targetIdList, targetIdListExcludingSelf);
    }

    @Override
    public Address selectLessee(Address lessorAddress) {
        int step = (int)(ThreadLocalRandom.current().nextDouble() * (MAX_SPAN-1)) + 1;
        int targetOperatorId = (step + partition.getThisOperatorIndex() + partition.getParallelism()) % partition.getParallelism();
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
        return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
    }

    @Override
    public Set<Address> selectLessees(Address lessorAddress, int count) {
        if(count >= partition.getParallelism()) throw new FlinkRuntimeException("Cannot select number of lessees greater than parallelism level.");
        HashSet<Integer> targetIds = new HashSet<>();
        for(int i = 0; i < count; i++){
            int targetOperatorId = (int)(ThreadLocalRandom.current().nextDouble() * (MAX_SPAN-1));
            targetOperatorId = (targetOperatorId + partition.getParallelism()) % partition.getParallelism();
            targetIds.add(targetOperatorId);
        }
        return targetIds.stream().map(targetOperatorId->{
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            return new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId));
        }).collect(Collectors.toSet());
    }

    @Override
    public void collect(Message message) { }

    @Override
    public ArrayList<Address> exploreLessee() {
        ArrayList<Address> ret = new ArrayList<>();
        Collections.shuffle(targetIdList);
        for(int i = 0; i < this.EXPLORE_RANGE; i++){
            int targetOperatorId = (targetIdList.get(i) + partition.getThisOperatorIndex() + partition.getParallelism()) % partition.getParallelism();
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)));
        }
        return ret;
    }

    @Override
    public ArrayList<Address> exploreTargetBasedLessee(Address target, Address source) {
        ArrayList<Address> ret = new ArrayList<>();
        int destinationOperatorIndex =
                assignKeyToParallelOperator(KeyBy.apply(target), partition.getMaxParallelism(), partition.getParallelism());
        Collections.shuffle(targetIdList);
        for(int i = 0; i < this.EXPLORE_RANGE; i++){
            int targetOperatorId = (targetIdList.get(i) + destinationOperatorIndex + partition.getParallelism()) % partition.getParallelism();
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)));
        }
        return ret;
    }

    @Override
    public ArrayList<Address> exploreLesseeWithBase(Address address) {
        ArrayList<Address> ret = new ArrayList<>();
        Collections.shuffle(targetIdList);
        for(int i = 0; i < this.EXPLORE_RANGE; i++){
            int targetOperatorId = (this.targetIdList.get(i) + partition.getThisOperatorIndex() + partition.getParallelism()) % partition.getParallelism();
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
        }
        return ret;
    }

    @Override
    public ArrayList<Address> getBroadcastAddresses(Address address){
        ArrayList<Address> ret = new ArrayList<>();
        for(int i : targetIdList){
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), i);
            if(!String.valueOf(keyGroupId).equals(address.id())){
                ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
            }
        }
        return ret;
    }

    @Override
    public Address selectLessee(Address lessorAddress, Address source) {
        return selectLessee(lessorAddress);
    }
}
