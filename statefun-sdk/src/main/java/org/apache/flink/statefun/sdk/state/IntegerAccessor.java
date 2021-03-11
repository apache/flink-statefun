package org.apache.flink.statefun.sdk.state;

public interface IntegerAccessor extends Accessor<Long> {
    Long incr();
}
