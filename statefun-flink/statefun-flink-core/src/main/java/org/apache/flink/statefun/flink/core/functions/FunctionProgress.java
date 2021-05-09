package org.apache.flink.statefun.flink.core.functions;

import java.io.Serializable;

public class FunctionProgress implements Serializable {
    public Long progressId;
    public Boolean sync;

    public FunctionProgress(Long progress, Boolean blocking){
        progressId = progress;
        sync = blocking;
    }
}
