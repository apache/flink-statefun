package org.apache.flink.statefun.sdk.state;

import org.apache.flink.statefun.sdk.Address;

public interface StateAccessDescriptor {
    Address getCurrentAccessor();

    Address getLessor();
}
