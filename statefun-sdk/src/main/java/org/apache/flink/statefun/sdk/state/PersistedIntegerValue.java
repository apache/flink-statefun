package org.apache.flink.statefun.sdk.state;

public class PersistedIntegerValue extends PersistedValue<Long>{
    private PersistedIntegerValue(String name, Class<Long> type, Expiration expiration, Accessor<Long> accessor) {
        super(name, type, expiration, accessor);
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
        Long updated = ((IntegerAccessor)accessor).incr();
        return updated;
    }

    private static final class NonFaultTolerantAccessor implements IntegerAccessor {
        private Long element;

        @Override
        public void set(Long element) {
            this.element = element;
        }

        @Override
        public Long get() {
            return element;
        }

        @Override
        public void clear() {
            element = null;
        }

        @Override
        public Long incr() {
            return element++;
        }
    }
}
