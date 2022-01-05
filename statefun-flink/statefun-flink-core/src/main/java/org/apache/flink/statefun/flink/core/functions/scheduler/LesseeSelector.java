package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.Partition;
import org.apache.flink.statefun.sdk.Address;

import java.util.ArrayList;
import java.util.Set;

public abstract class LesseeSelector {

    protected Partition partition;

    public abstract Address selectLessee(Address lessorAddress);

    public abstract Set<Address> selectLessees(Address lessorAddress, int count);

    public void collect(Address address, Integer queueSize){ }

    public abstract ArrayList<Address> exploreLessee(Address address);

    public abstract ArrayList<Address> exploreLesseeWithBase(Address address);

    public ArrayList<Address> getBroadcastAddresses(Address address){
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < partition.getParallelism(); i++){
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), i);
            ret.add(new Address(address.type(), String.valueOf(keyGroupId)));
        }
        return ret;
    }
}
