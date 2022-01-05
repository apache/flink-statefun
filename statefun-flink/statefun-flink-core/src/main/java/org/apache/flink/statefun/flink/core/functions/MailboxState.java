package org.apache.flink.statefun.flink.core.functions;

public class MailboxState {

    public FunctionActivation.Status status;

    public boolean readyToBlock;

    public boolean pendingStateRequest;

    public MailboxState(FunctionActivation.Status status, boolean readyToBlock, boolean pendingStateRequest) {
        this.status = status;
        this.readyToBlock = readyToBlock;
        this.pendingStateRequest = pendingStateRequest;
    }
}
