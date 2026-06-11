package com.minibroker.raft;

public class NotLeaderException extends Exception {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("Not leader. Current leader is: " + leaderId);
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }
}
