package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedUnsafeWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;

final public class PBSchedulingStrategy extends SchedulingStrategy {

    public PBSchedulingStrategy(){ }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }

    @Override
    public WorkQueue createWorkQueue() {
        return new PriorityBasedUnsafeWorkQueue();
    }

}
