package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.state.StateAccessDescriptor;

import java.util.ArrayList;

public interface PartitionedMergeableState  {
    void setAllRemotePartitionedAccessors(ArrayList remoteAccessors);
    void mergeAllPartition();
    Integer getNumPartitions();
    void fromByteArray(byte[] array);
    byte[] toByteArray();
    StateAccessDescriptor getStateAccessDescriptor();
}
