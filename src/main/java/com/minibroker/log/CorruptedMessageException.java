package com.minibroker.log;

public class CorruptedMessageException extends RuntimeException {

    public CorruptedMessageException(String message) {
        super(message);
    }
}
