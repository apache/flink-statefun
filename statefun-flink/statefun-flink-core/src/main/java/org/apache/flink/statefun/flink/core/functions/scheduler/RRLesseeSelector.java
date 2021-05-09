package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.Partition;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;



public class RRLesseeSelector extends LesseeSelector {
    private  int EXPLORE_RANGE = 1;
    private transient static final Logger LOG = LoggerFactory.getLogger(RandomLesseeSelector.class);
    //    private ThreadLocalRandom random;
    int lastIndex = 0;
    int lastExplored = 0;

    public RRLesseeSelector(Partition partition) {
//        this.random = new Random();
        this.partition = partition;
        LOG.debug("Initialize RRLesseeSelector operator index {} parallelism {} max parallelism {} keygroup {}",
                partition.getThisOperatorIndex(), partition.getParallelism(), partition.getMaxParallelism(),
                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), partition.getThisOperatorIndex()));
    }

    @Override
    public Address selectLessee(Address lessorAddress) {
//        ArrayList<Address> ret = new ArrayList<>();
//        int targetOperatorId = ((ThreadLocalRandom.current().nextInt()%(partition.getParallelism()-1) + (partition.getParallelism()-1))%(partition.getParallelism()-1) + partition.getThisOperatorIndex() + 1)%(partition.getParallelism());
////        int targetOperatorId = (random.nextInt()%partition.getParallelism() + partition.getParallelism())%partition.getParallelism();
////        while(targetOperatorId == partition.getThisOperatorIndex()){
////            targetOperatorId = (random.nextInt()%partition.getParallelism() + partition.getParallelism())%partition.getParallelism();
////        }
        if (lastIndex == this.partition.getThisOperatorIndex()){
            lastIndex = (lastIndex + 1) % this.partition.getParallelism();
        }
        int targetOperatorId = lastIndex;
        lastIndex = (lastIndex + 1) % this.partition.getParallelism();
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);

        return new Address(lessorAddress.type(), String.valueOf(keyGroupId));

    }

    @Override
    public Set<Address> selectLessees(Address lessorAddress, int count) {
        if(count >= partition.getParallelism()) throw new FlinkRuntimeException("Cannot select number of lessees greater than parallelism level.");
        HashSet<Integer> targetIds = new HashSet<>();
        for(int i = 0; i < count; i++){
//            int targetOperatorId = ((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
//            while(targetIds.contains(targetOperatorId)){
//                targetOperatorId = ((ThreadLocalRandom.current().nextInt() % (partition.getParallelism() - 1) + (partition.getParallelism() - 1)) % (partition.getParallelism() - 1) + partition.getThisOperatorIndex() + 1) % (partition.getParallelism());
//            }
            if (lastIndex == this.partition.getThisOperatorIndex()){
                lastIndex = (lastIndex + 1) % this.partition.getParallelism();
            }
            targetIds.add(lastIndex);
            lastIndex = (lastIndex + 1) % this.partition.getParallelism();
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
        for(int i = 0; i < EXPLORE_RANGE; i++){
            if (lastExplored == this.partition.getThisOperatorIndex()){
                lastExplored = (lastExplored + 1) % this.partition.getParallelism();
            }
            int targetOperatorId =lastExplored;
            lastExplored = (lastExplored + 1) % this.partition.getParallelism();
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)));
        }
        return ret;
    }

    @Override
    public ArrayList<Address> exploreLesseeWithBase(Address address) {
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < EXPLORE_RANGE; i++){
            if (lastExplored == this.partition.getThisOperatorIndex()){
                lastExplored = (lastExplored + 1) % this.partition.getParallelism();
            }
            int targetOperatorId =lastExplored;
            lastExplored = (lastExplored + 1) % this.partition.getParallelism();
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), targetOperatorId);
            ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
        }
        return ret;
    }

}
