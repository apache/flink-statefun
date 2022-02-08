package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.InternalAddress;

import java.util.*;
import java.util.stream.Collectors;

// Class to hold all the information related to state aggregation
public class StateAggregationInfo {
    //        private int numUpstreams;
    //private int numPartialStatesReceived;
    private Address syncSource;
    private Set<InternalAddress> lessees;
    private ArrayList<Address> partitionedAddresses;
    private LesseeSelector lesseeSelector;
    private HashSet<InternalAddress> distinctPartialStateSources;
    private Set<InternalAddress> expectedPartialStateSources;
    private HashSet<InternalAddress> distinctCriticalMessages;
    // TODO: Assigned from sync recv
    public Set<InternalAddress> expectedCriticalMessageSources;
    private Boolean autoblocking;
    private Boolean pendingRequestServed;


    public StateAggregationInfo(ReusableContext context) {
//            this.numUpstreams = numUpstreams;
        this.syncSource = null;
        this.lessees = new HashSet<>();
        //this.numPartialStatesReceived = 0;
        this.partitionedAddresses = null;
        this.lesseeSelector = new RandomLesseeSelector(context.getPartition());
        this.distinctPartialStateSources = new HashSet<>();
        this.expectedPartialStateSources = new HashSet<>();
        this.distinctCriticalMessages = new HashSet<>();
        this.expectedCriticalMessageSources = new HashSet<>();
        this.autoblocking = null;
        this.pendingRequestServed = true;
    }

    public void resetInfo() {
        this.distinctPartialStateSources.clear();
        this.distinctCriticalMessages.clear();
        this.expectedCriticalMessageSources.clear();
        this.expectedPartialStateSources.clear();
        this.lessees.clear();
        this.autoblocking = null;
        this.syncSource = null;
    }

    public Address getSyncSource() {
        return this.syncSource;
    }

    public void setSyncSource(Address syncSource) {
        this.syncSource = syncSource;
    }
//        public int getNumUpstreams() {
//            return this.numUpstreams;
//        }

    public void incrementNumPartialStatesReceived(InternalAddress address) {
        this.distinctPartialStateSources.add(address);
        //this.numPartialStatesReceived += 1;
    }

    public void incrementNumCriticalMessagesReceived(InternalAddress address) {
        this.distinctCriticalMessages.add(address);
    }

    public boolean areAllPartialStatesReceived() {
        //return (this.distinctPartialStateSources.size() == lesseeSelector.getBroadcastAddresses(lessor).size());
        return (this.distinctPartialStateSources.size() == expectedPartialStateSources.size());
    }

    public boolean areAllCriticalMessagesReceived() {
        return (this.distinctCriticalMessages.size() == expectedCriticalMessageSources.size());
    }

    public void setExpectedPartialStateSources(Set<InternalAddress> sources) {
        expectedPartialStateSources = sources;
    }

    public Set<Address> getExpectedPartialStateSources() {
        return expectedPartialStateSources.stream().map(x -> x.address).collect(Collectors.toSet());
    }

    public Set<Address> getExpectedCriticalMessage() {
        return expectedCriticalMessageSources.stream().map(x -> x.address).collect(Collectors.toSet());
    }

    public void setExpectedCriticalMessageSources(Set<InternalAddress> sources) {
        expectedCriticalMessageSources = sources;
    }

    public void addLessee(Address lessee) {
        lessees.add(new InternalAddress(lessee, lessee.type().getInternalType()));
    }

    public List<Address> getLessees() {
        return lessees.stream().map(ia -> ia.address).collect(Collectors.toList());
    }

    public boolean hasLessee(Address lessee) {
        return lessees.contains(new InternalAddress(lessee, lessee.type().getInternalType()));
    }

    public void setAutoblocking(Boolean blocking) {
        autoblocking = blocking;
    }

    public Boolean ifAutoblocking() {
        return autoblocking;
    }

    public Boolean getPendingRequestServed() {
        return pendingRequestServed;
    }

    public void setPendingRequestServed(Boolean requestServed) {
        pendingRequestServed = requestServed;
    }

    // TODO: Use this function at all context forwards. Need to capture the context.forward() call
    public void addPartition(Address partition) {
        this.partitionedAddresses.add(partition);
    }

    public ArrayList<Address> getPartitionedAddresses() {
        //return this.partitionedAddresses;
        return lesseeSelector.getBroadcastAddresses(syncSource);
    }
    public HashSet<InternalAddress> getDistinctPartialStateSources() {
        return distinctPartialStateSources;
    }

    public void setDistinctPartialStateSources(HashSet<InternalAddress> distinctPartialStateSources) {
        this.distinctPartialStateSources = distinctPartialStateSources;
    }

    public HashSet<InternalAddress> getDistinctCriticalMessages() {
        return distinctCriticalMessages;
    }

    public void setDistinctCriticalMessages(HashSet<InternalAddress> distinctCriticalMessages) {
        this.distinctCriticalMessages = distinctCriticalMessages;
    }

    public Set<InternalAddress> getExpectedCriticalMessageSources() {
        return expectedCriticalMessageSources;
    }

    @Override
    public String toString() {
        return String.format("StateAggregationInfo numPartialStatesReceived %d lessor %s partitionedAddresses %s hash %d", distinctPartialStateSources.size(),
                (syncSource == null ? "null" : syncSource.toString()), (partitionedAddresses == null ? "null" : Arrays.toString(partitionedAddresses.toArray())), this.hashCode());
    }


}
