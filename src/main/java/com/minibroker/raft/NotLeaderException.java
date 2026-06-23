package com.minibroker.raft;

public class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("Not leader. Current leader is: " + leaderId);
        this.leaderId = leaderId;
    }

    public NotLeaderException(String message, Throwable cause) {
        super(message, cause);
        this.leaderId = null;
    }

    public NotLeaderException(String message, String leaderId) {
        super(message);
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }
}
