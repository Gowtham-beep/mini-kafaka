package com.minibroker;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.FetchPurgatory;
import com.minibroker.raft.RaftNode;
import com.minibroker.raft.RequestPurgatory;
import com.minibroker.raft.rpc.DefaultElectionTimer;
import com.minibroker.raft.rpc.RaftRpcClient;
import com.minibroker.raft.rpc.RaftServer;

public class MiniKafkaBroker implements Broker {

    private final BrokerConfig config;
    private final SegmentedLog log;
    private final RequestPurgatory requestPurgatory;
    private final FetchPurgatory fetchPurgatory;
    private final RaftRpcClient rpcClient;
    private final DefaultElectionTimer electionTimer;
    private final RaftNode raftNode;
    private final RaftServer server;

    public MiniKafkaBroker(BrokerConfig config) {
        this.config = config;

        try {
            this.log = new SegmentedLog(0, config.dataDir());
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to initialize SegmentedLog", e);
        }

        this.requestPurgatory = new RequestPurgatory();
        this.fetchPurgatory = new FetchPurgatory();

        Map<String, InetSocketAddress> peers = config.clusterAddresses().entrySet().stream()
            .filter(e -> !e.getKey().equals(config.nodeId()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        this.rpcClient = new RaftRpcClient(peers);
        this.electionTimer = new DefaultElectionTimer();

        List<String> peerIds = new ArrayList<>(peers.keySet());

        this.raftNode = new RaftNode(
            config.nodeId(),
            log,
            rpcClient,
            electionTimer,
            requestPurgatory,
            fetchPurgatory,
            peerIds
        );

        this.server = new RaftServer(config.port(), raftNode);
    }

    @Override
    public void start() {
        electionTimer.setNode(raftNode);
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start RaftServer", e);
        }
        electionTimer.reset();
    }

    @Override
    public void shutdown() {
        // 1. Stop inbound network server first. This prevents any new incoming RPCs from arriving 
        //    and trying to route work into executors that might be dead.
        server.shutDown();

        // 2. Stop consensus executors safely. Since no new inbound traffic is arriving, 
        //    the active state machine threads can safely drain their current work and halt.
        raftNode.shutDown();

        // 3. Stop outbound network client. With executors halted, no new outbound requests 
        //    will be generated, so we can sever connections to peers.
        rpcClient.shutDown();

        // 4. Stop schedulers. No more elections or heartbeats need to be fired now that 
        //    the node is offline.
        electionTimer.shutDown();

        // 5. Close disk resources last. This guarantees that any in-flight work that managed 
        //    to run before the executors halted had a full chance to flush its writes safely.
        log.close();
    }

    @Override
    public Role getRole() {
        RaftNode.NodeState state = raftNode.getNodeState();
        if (state == RaftNode.NodeState.LEADER) return Role.LEADER;
        if (state == RaftNode.NodeState.CANDIDATE) return Role.CANDIDATE;
        return Role.FOLLOWER;
    }

    @Override
    public BrokerMetrics getMetrics() {
        return new BrokerMetrics(
            raftNode.getCommitIndex(),
            0,
            0 
        );
    }

    @Override
    public InetSocketAddress getLeaderAddress() {
        String leaderId = raftNode.getLeaderId();
        if (leaderId == null) {
            return null;
        }
        return config.clusterAddresses().get(leaderId);
    }
}
