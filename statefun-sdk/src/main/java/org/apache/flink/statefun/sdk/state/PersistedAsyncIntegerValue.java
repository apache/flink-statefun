package org.apache.flink.statefun.sdk.state;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.CompletableFuture;

public class PersistedAsyncIntegerValue extends PersistedAsyncValue<Long> {
    protected PersistedAsyncIntegerValue(String name, Class<Long> type, Expiration expiration, AsyncAccessor<Long> accessor, Boolean nft) {
        super(name, type, expiration, accessor, nft);
    }

    /**
     * Creates a {@link PersistedAsyncIntegerValue} instance that may be used to access persisted state managed by
     * the system. Access to the persisted value is identified by an unique name and type of the
     * value. These may not change across multiple executions of the application.
     *
     * @param name the unique name of the persisted state.
     * @return a {@code PersistedValue} instance.
     */
    public static PersistedAsyncIntegerValue of(String name) {
        return of(name, Expiration.none());
    }

    public static PersistedAsyncIntegerValue of(String name, Boolean nonFaultTolerant) {
        return of(name, Expiration.none(), nonFaultTolerant);
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
    public static PersistedAsyncIntegerValue of(String name, Expiration expiration) {
        return new PersistedAsyncIntegerValue(name, Long.class, expiration, new NonFaultTolerantAccessor(), false);
    }

    public static PersistedAsyncIntegerValue of(String name, Expiration expiration, Boolean nftFlag) {
        return new PersistedAsyncIntegerValue(name, Long.class, expiration, new NonFaultTolerantAccessor(), nftFlag);
    }

    public CompletableFuture<Long> increment() {
        CompletableFuture<Long> updated = ((AsyncIntegerAccessor)accessor).incrAsync();
        return updated;
    }

    private static final class NonFaultTolerantAccessor implements AsyncIntegerAccessor {
        private Long element;

        @Override
        public CompletableFuture<String> setAsync(Long element) {
            this.element = element;
            return CompletableFuture.completedFuture("");
        }

        @Override
        public CompletableFuture<Long> getAsync() {
            return CompletableFuture.completedFuture(element);
        }

        @Override
        public void set(Long value) {
            throw new NotImplementedException();
        }

        @Override
        public Long get() {
            throw new NotImplementedException();
        }

        @Override
        public void clear() {
            element = null;
        }

        @Override
        public CompletableFuture<Long> incrAsync() {
            return CompletableFuture.completedFuture(element++);
        }
    }
}
