package org.apache.flink.statefun.sdk.state;

public interface CachedAccessor{
    boolean ifActive();
    void setActive(boolean active);
    void verifyValid();
}
