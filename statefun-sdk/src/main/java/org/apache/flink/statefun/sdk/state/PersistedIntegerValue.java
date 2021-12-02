package org.apache.flink.statefun.sdk.state;

import java.util.Objects;

public class PersistedIntegerValue extends PersistedValue<Long>{
    private PersistedIntegerValue(String name, Class<Long> type, Expiration expiration, IntegerAccessor accessor) {
        super(name, type, expiration, accessor, false);
    }

    /**
     * Creates a {@link PersistedAsyncIntegerValue} instance that may be used to access persisted state managed by
     * the system. Access to the persisted value is identified by an unique name and type of the
     * value. These may not change across multiple executions of the application.
     *
     * @param name the unique name of the persisted state.
     * @return a {@code PersistedValue} instance.
     */
    public static PersistedIntegerValue of(String name) {
        return of(name, Expiration.none());
    }

    /**
     * Creates a {@link PersistedValue} instance that may be used to access persisted state managed by
     * the system. Access to the persisted value is identified by an unique name and type of the
     * value. These may not change across multiple executions of the application.
     *
     * @param name the unique name of the persisted state.
     * @param expiration state expiration configuration.
     * @return a {@code PersistedValue} instance.
     */
    public static PersistedIntegerValue of(String name, Expiration expiration) {
        return new PersistedIntegerValue(name, Long.class, expiration, new PersistedIntegerValue.NonFaultTolerantAccessor());
    }

    public Long increment() {
        Long updated = ((NonFaultTolerantAccessor)cachingAccessor).incr();
        return updated;
    }

    @Override
    public void setAccessor(Accessor<Long> newAccessor) {
//    if(this.nonFaultTolerant) return;
        this.accessor = Objects.requireNonNull(newAccessor);
        ((NonFaultTolerantAccessor)this.cachingAccessor).initialize((IntegerAccessor) this.accessor);
    }

    @Override
    public void setInactive() { ((NonFaultTolerantAccessor)this.cachingAccessor).setActive(false); }

    private static final class NonFaultTolerantAccessor implements IntegerAccessor, CachedAccessor {
        private Long element;
        private IntegerAccessor remoteAccessor;
        private boolean active;
        private boolean modified;

        public void initialize(IntegerAccessor remote){
            remoteAccessor = remote;
            element = remoteAccessor.get();
            active = true;
        }

        @Override
        public void set(Long element) {
            verifyValid();
            this.element = element;
            modified = true;
        }

        @Override
        public Long get() {
            verifyValid();
            return element;
        }

        @Override
        public void clear() {
            element = null;
            modified = true;
        }

        @Override
        public Long incr() {
            verifyValid();
            modified = true;
            return element++;
        }

        @Override
        public boolean ifActive() {
            verifyValid();
            return active;
        }

        @Override
        public boolean ifModified() {
            return modified;
        }

        @Override
        public void setActive(boolean active) {
            verifyValid();
            this.active = active;
        }

        @Override
        public void verifyValid() {
            if(!active){
                initialize(this.remoteAccessor);
            }
        }
    }
}
