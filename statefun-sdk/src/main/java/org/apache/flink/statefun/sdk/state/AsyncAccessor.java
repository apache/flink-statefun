package org.apache.flink.statefun.sdk.state;

import java.util.concurrent.CompletableFuture;

public interface AsyncAccessor<T> extends Accessor<T>{

    CompletableFuture<String> setAsync(T value);

    CompletableFuture<T> getAsync();
}
