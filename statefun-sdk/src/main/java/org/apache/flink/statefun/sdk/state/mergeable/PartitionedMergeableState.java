package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.statefun.sdk.state.ManagedState;

import java.util.ArrayList;

public interface PartitionedMergeableState  {

    void setAllRemotePartitionedAccessors(ArrayList remoteAccessors);
    void mergeAllPartition();
    Integer getNumPartitions();
}
