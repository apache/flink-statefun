package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.sdk.Address;

import java.util.ArrayList;

public abstract class SpanLesseeSelector extends LesseeSelector{

    public abstract Address selectLessee(Address lessorAddress, Address source);

    public abstract ArrayList<Address> exploreTargetBasedLessee(Address target, Address source);

    public abstract ArrayList<Address> lesseeIterator(Address address);
}
