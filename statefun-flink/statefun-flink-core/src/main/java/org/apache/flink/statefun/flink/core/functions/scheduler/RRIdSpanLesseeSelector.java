package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.commons.lang3.NotImplementedException;
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

public class RRIdSpanLesseeSelector extends SpanLesseeSelector {
    private int EXPLORE_RANGE = 1;
    private int MAX_SPAN = 4;

    //private int lastIndex = 0;
    private transient static final Logger LOG = LoggerFactory.getLogger(RRIdSpanLesseeSelector.class);
    private final List<Integer> targetIdList;
    private final List<Integer> targetIdListExcludingSelf;
    private final HashMap<String, Integer> lessorToLastIndex;

    public RRIdSpanLesseeSelector(Partition partition, int range, int searchSpan) {
        this.EXPLORE_RANGE = range;
        this.MAX_SPAN = searchSpan;
        this.partition = partition;
        this.targetIdList = new ArrayList<>();
        this.targetIdListExcludingSelf = new ArrayList<>();
        this.lessorToLastIndex = new HashMap<>();
        for (int i = 0; i < MAX_SPAN; i++){
            this.targetIdList.add(i);
        }
        for (int i = 1; i < MAX_SPAN; i++){
            this.targetIdListExcludingSelf.add(i);
        }
        if(EXPLORE_RANGE > MAX_SPAN){
            throw new FlinkRuntimeException(String.format("EXPLORE_RANGE %d cannot be greater MAX_SPAN %d \n", EXPLORE_RANGE, MAX_SPAN));
        }
        LOG.info("Initialize RoundRobinIdSpanLesseeSelector operator index {} parallelism {} max parallelism {} keygroup id list {} EXPLORE_RANGE {} MAX_SPAN {} targetIdList {} targetIdListExcludingSelf {}",
                partition.getThisOperatorIndex(), partition.getParallelism(), partition.getMaxParallelism(),
                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(),partition.getThisOperatorIndex()),
                        EXPLORE_RANGE, MAX_SPAN, targetIdList, targetIdListExcludingSelf);
    }

    @Override
    public Address selectLessee(Address lessorAddress) {
        String lessorKey = lessorAddress.toString();
        int lastIndex = lessorToLastIndex.getOrDefault(lessorKey, 0);
//        Collections.shuffle(targetIdListExcludingSelf);
        int targetOperatorId = targetIdListExcludingSelf.get(lastIndex);//(step + partition.getThisOperatorIndex() + partition.getParallelism()) % partition.getParallelism();
        lastIndex = (lastIndex + 1 + MAX_SPAN - 1 ) % (MAX_SPAN-1);
        lessorToLastIndex.put(lessorKey, lastIndex);
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
        return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
    }

    @Override
    public Set<Address> selectLessees(Address lessorAddress, int count) {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public void collect(Message message) { }

    @Override
    public ArrayList<Address> exploreLessee() {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public ArrayList<Address> exploreTargetBasedLessee(Address target, Address source) {
        ArrayList<Address> ret = new ArrayList<>();
        int destinationOperatorIndex =
                assignKeyToParallelOperator(KeyBy.apply(target), partition.getMaxParallelism(), partition.getParallelism());
        String key = target.toString() + " " + source.toString();
        // Collections.shuffle(targetIdList);
        for(int i = 0; i < this.EXPLORE_RANGE; i++){
            int rangeIndex = (lessorToLastIndex.getOrDefault(key, 0) + i + targetIdList.size()) % targetIdList.size();
            int targetOperatorId = (targetIdList.get(rangeIndex) + destinationOperatorIndex + partition.getParallelism()) % partition.getParallelism();
            //((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)));
        }
        lessorToLastIndex.putIfAbsent(key, 0);
        lessorToLastIndex.compute(key, (k,v)-> (v + 1 +targetIdList.size()) % targetIdList.size());
        // LOG.debug("Select explore targets {} this index {} key {}", ret.toArray(), partition.getThisOperatorIndex(), key);
        return ret;
    }

    @Override
    public ArrayList<Address> exploreLesseeWithBase(Address address) {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public ArrayList<Address> getBroadcastAddresses(Address address){
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public Address selectLessee(Address lessorAddress, Address source) {
        String lessorKey = lessorAddress.toString() + " " + source.toString();
        int lastIndex = lessorToLastIndex.getOrDefault(lessorKey, 0);
//        Collections.shuffle(targetIdListExcludingSelf);
        int targetOperatorId = targetIdListExcludingSelf.get(lastIndex);//(step + partition.getThisOperatorIndex() + partition.getParallelism()) % partition.getParallelism();
        lastIndex = (lastIndex + 1 + MAX_SPAN - 1 ) % (MAX_SPAN-1);
        lessorToLastIndex.put(lessorKey, lastIndex);
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
        return new Address(lessorAddress.type(), String.valueOf(keyGroupId));
    }
}
