#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    m<<args.term<<args.candidateId<<args.lastLogIndex<<args.lastLogTerm;
    return m;
}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    u>>args.term>>args.candidateId>>args.lastLogIndex>>args.lastLogTerm;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    m<<reply.currentTerm<<reply.vateGranted;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    u>>reply.currentTerm>>reply.vateGranted;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m<<reply.success<<reply.term<<reply.conflictIndex;
    return m;
}
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply) {
    // Your code here
    m>>reply.success>>reply.term>>reply.conflictIndex;
    return m;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    // Your code here
    m << args.term << args.lastIncludedIndex << args.lastIncludedTerm << args.data;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    // Your code here
    u >> args.term >> args.lastIncludedIndex >> args.lastIncludedTerm >> args.data;
    return u; 
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m<<reply.term;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u>>reply.term;
    return u;
}