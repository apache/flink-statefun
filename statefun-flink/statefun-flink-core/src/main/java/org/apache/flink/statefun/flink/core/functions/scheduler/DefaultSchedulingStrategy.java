package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.statefun.flink.core.functions.utils.DefaultWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;

public class DefaultSchedulingStrategy extends SchedulingStrategy {
    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }

    @Override
    public void createWorkQueue() {
        pending = new DefaultWorkQueue();
    }
}
