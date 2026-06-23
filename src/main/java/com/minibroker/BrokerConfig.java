package com.minibroker;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;

public record BrokerConfig(
    String nodeId,
    Path dataDir,
    int port,
    Map<String, InetSocketAddress> clusterAddresses // Must contain ALL nodes, including self
) {}
