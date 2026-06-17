package com.minibroker.raft.rpc;

public sealed interface RaftMessage 
    permits AppendEntriesRequest, AppendEntrieResponse, RequestVoteRequest, RequestVoteResponse{
    
}
