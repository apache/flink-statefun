package org.apache.flink.statefun.sdk.state;

import java.util.concurrent.CompletableFuture;

public abstract class AsyncAccessor<T> implements Accessor<T>{

    public abstract CompletableFuture<String> setAsync(T value);

    public abstract CompletableFuture<T> getAsync();
}
