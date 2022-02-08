package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.sdk.Address;

public class MailboxState {

    public FunctionActivation.Status status;

    public boolean readyToBlock;

    public Address pendingStateRequest;

    public MailboxState(FunctionActivation.Status status, boolean readyToBlock, Address pendingStateRequest ) {
        this.status = status;
        this.readyToBlock = readyToBlock;
        this.pendingStateRequest = pendingStateRequest;
    }
}
