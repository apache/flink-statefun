package org.apache.flink.statefun.sdk.state;

public interface CachedAccessor{
    boolean ifActive();
    boolean ifModified();
    void setActive(boolean active);
    void verifyValid();
}
