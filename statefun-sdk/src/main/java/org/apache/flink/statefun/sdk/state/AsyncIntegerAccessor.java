package org.apache.flink.statefun.sdk.state;

import java.util.concurrent.CompletableFuture;

public interface AsyncIntegerAccessor extends AsyncAccessor<Long>{
    CompletableFuture<Long> incrAsync();
}
