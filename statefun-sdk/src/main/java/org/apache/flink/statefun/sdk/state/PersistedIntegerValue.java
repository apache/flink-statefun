package org.apache.flink.statefun.sdk.state;

public class PersistedIntegerValue extends PersistedValue<Long>{
    private PersistedIntegerValue(String name, Class<Long> type, Expiration expiration, Accessor<Long> accessor) {
        super(name, type, expiration, accessor);
    }

    public Long increment() {
        Long updated = ((IntegerAccessor)accessor).incr();
        return updated;
    }
}
