#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"
#include <string>
enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    int term;           //candidate's term
    int candidateId;    //candidate requesting vote
    int lastLogIndex;   //index of candidate’s last log entry
    int lastLogTerm;    //term of candidate’s last log entry
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    int currentTerm;     //currentTerm, for candidate to update itself
    bool vateGranted;    //true means candidate received vote
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    command command_;
    int termNumber;
    int index;
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m<<entry.command_<<entry.termNumber<<entry.index;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u>>entry.command_>>entry.termNumber>>entry.index;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;                               //leader’s term
    int leaderId;                           //so follower can redirect clients
    int prevLogIndex;                       //index of log entry immediately preceding new ones
    int prevLogTerm;                        //term of prevLogIndex entry
    bool empty;
    std::vector<log_entry<command>>  entries;        //log entries to store (empty for heartbeat;may send more than one for efficiency)
    int leaderCommit;                       //leader’s commitIndex
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m<<args.term<<args.leaderId<<args.prevLogIndex<<args.prevLogTerm<<args.empty<<args.leaderCommit<<args.entries;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u>>args.term>>args.leaderId>>args.prevLogIndex>>args.prevLogTerm>>args.empty>>args.leaderCommit>>args.entries;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term; //currentTerm for leader to update itself
    bool success;// true if follower contained entry matching prevLogIndex and prevLogTerm
    int conflictIndex;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
    int term;
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<char> data;
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
    int term;
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h