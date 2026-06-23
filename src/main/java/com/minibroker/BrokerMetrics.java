package com.minibroker;

public record BrokerMetrics(
    long commitIndex,
    long logSizeBytes,
    int activeRequests
) {}
