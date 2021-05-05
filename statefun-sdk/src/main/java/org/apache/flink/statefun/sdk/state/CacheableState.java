package org.apache.flink.statefun.sdk.state;

public interface CacheableState {
    void markDirty();
    boolean isDirty();
    void sync() throws Exception;
}
