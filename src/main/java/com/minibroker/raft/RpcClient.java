package com.minibroker.raft;

import java.util.concurrent.CompletableFuture;
import com.minibroker.raft.rpc.AppendEntriesRequest;
import com.minibroker.raft.rpc.AppendentrieResponse;
import com.minibroker.raft.rpc.RequestVoteRequest;
import com.minibroker.raft.rpc.RequestVoteResponse;

public interface RpcClient {
    CompletableFuture<RequestVoteResponse> sendrequestVote(String peer, RequestVoteRequest request);
    CompletableFuture<AppendentrieResponse> sendAppendEntries(String peer, AppendEntriesRequest request);
}
