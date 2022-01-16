#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <sys/time.h>
#include <iostream>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);
#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << ":" << __LINE__ << " msg: " << message << std::endl; \
            std::terminate(); \
        } \
    } while (false)

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;

    int my_id;                     // The index of this node in rpc_clients, start from 0

    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };

    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;


    // Your code here:
    int commitIndex;
    int lastApplied;
    int followCount;
    int my_timeout_follower;
    // int my_timeout_candidate;
    long long last_RPC_time;
    int voteFor;


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void persistMetadata();
    // void persistLog();
    void readPersist();
    void renewRPCtime();
    void sendHeartBeat();
    int randomTimeout();

};

template<typename state_machine, typename command>
void raft<state_machine, command>::persistMetadata(){
    storage->saveMetaDataToDir(current_term);
}

template<typename state_machine, typename command>
int raft<state_machine, command>::randomTimeout(){
    int randomSeed = rand() + rand() % 500;
    srand(randomSeed);
    return  300 + (rand() % 400);
    //from internet
}
template<typename state_machine, typename command>
void raft<state_machine, command>::renewRPCtime(){
    timeval current;
	gettimeofday(&current, NULL);
    last_RPC_time = current.tv_sec*1000 + current.tv_usec/1000;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::sendHeartBeat(){
    if(role != leader)return;
    append_entries_args<command> arg;
    arg.term = current_term;
    arg.empty = true;
    arg.leaderId = my_id;
    arg.leaderCommit = commitIndex;
        
    arg.prevLogIndex =storage->back().index;
    arg.prevLogTerm = storage->back().termNumber;
            
    for(int i = 0; i < (int)rpc_clients.size(); i++){
        if(i == my_id) continue;
        send_append_entries(i, arg);
    }
}

// template<typename state_machine, typename command>
// void raft<state_machine, command>::persistLog(){
//     storage->saveLogToDir();
// }

template<typename state_machine, typename command>
void raft<state_machine, command>::readPersist(){
    current_term = storage->readCurrent_term();
    lastApplied = storage->readSnapShotIndexFromDir();
    commitIndex = storage->readSnapShotIndexFromDir();
    if(lastApplied)
        (static_cast<raft_state_machine*>(state))->apply_snapshot(storage->getSnapshotData());
}

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    commitIndex(0),
    lastApplied(0)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    int randomSeed = rand() + rand() % 500;
    srand(randomSeed);
    my_timeout_follower =  300 + (rand() % 400);
    // my_timeout_candidate =  1000;
    last_RPC_time = 0;
    readPersist();
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    /* 如果是leader接受，否则拒绝 */
    
    std::lock_guard<std::mutex> guard(mtx);
    
    if(role != raft_role::leader){
        RAFT_LOG("I received a command but I am not a leader");
        return false;
    }

    index = storage->currentLatestIndex()+1;
    term = current_term;
    storage->setNextIndexs(my_id, index+1);
    storage->setMatchedIndexs(my_id, index);
    log_entry<command> entry;
    entry.command_ = cmd;
    entry.index = index;
    entry.termNumber = term;
    storage->pushBackEntry(entry);

    RAFT_LOG("I received a command and I am a leader.The comma d's index is %d", index);
    persistMetadata();
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    std::lock_guard<std::mutex> guard(mtx);

    storage->snapshot_index = lastApplied;
    std::vector<char> snapshot = (static_cast<raft_state_machine*>(state))->snapshot();
    storage->setSnapshotData(snapshot);
    storage->eraseBeforeLastIncluded(lastApplied);
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    std::lock_guard<std::mutex> guard(mtx);

    reply.vateGranted = false;
    reply.currentTerm = current_term;

    if(args.term <= current_term){
        RAFT_LOG("I refuse %d. It's term is %d.", args.candidateId, args.term);
        return 0;
    }//args.term > current_term
    if(args.term > current_term){
        role = follower;
        current_term = args.term;
        persistMetadata();
        RAFT_LOG("I renew my term.");
    }
    
    if(args.lastLogTerm < storage->back().termNumber){
        RAFT_LOG("I refuse %d. My lastLogTerm is %d, its is %d.", args.candidateId, storage->back().termNumber, args.lastLogTerm);
        return 0;
    }
    if(args.lastLogTerm == storage->back().termNumber && args.lastLogIndex < storage->back().index){
        RAFT_LOG("I refuse %d. My lastLogIndex is %d, its is %d.", args.candidateId, storage->back().index, args.lastLogIndex);
        return 0;
    }
    
    RAFT_LOG("I accept %d.I am a follower now.", args.candidateId);
    reply.vateGranted = true;
    role = follower;
    renewRPCtime();
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:

    if(arg.term > current_term){
        mtx.lock();
        role = follower;
        current_term = arg.term;
        mtx.unlock();
        RAFT_LOG("I renew my term.");
    }

    if(reply.vateGranted && role == raft_role::candidate){
        followCount ++;
        RAFT_LOG("I get vote from %d.Current vote I get is %d.", target, followCount);
        if(followCount >= ((int)rpc_clients.size()+1)/2){
            RAFT_LOG("I am leader now.");
            mtx.lock();

            storage->initializeNextIndexs(rpc_clients.size(), storage->currentLatestIndex()+1);
            storage->initializeMatchedIndexs(rpc_clients.size(), 0);
            role = leader;

            mtx.unlock();
            sendHeartBeat();
        }
    }
    persistMetadata();
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    std::lock_guard<std::mutex> guard(mtx);

    renewRPCtime();

    if(arg.term >= current_term){
        role = follower;
        current_term = arg.term;
        RAFT_LOG("I receive %d's heartbeat. I am follower now!", arg.leaderId);
    }

    reply.term = current_term;

    if(arg.term < current_term){
        reply.success = false;
        persistMetadata();
        return 0;
    }

    reply.success = true;

    if(arg.prevLogIndex < storage->snapshot_index || arg.prevLogIndex > storage->currentLatestIndex() || storage->getEntryByIndex(arg.prevLogIndex).termNumber != arg.prevLogTerm){
        if(arg.prevLogIndex < storage->snapshot_index || arg.prevLogIndex > storage->currentLatestIndex()){
            reply.conflictIndex = storage->currentLatestIndex() + 1;
            RAFT_LOG("I refuse %d's command. Because arg.prevLogIndex = %d, storage->currentLatestIndex() = %d ", arg.leaderId, arg.prevLogIndex, storage->currentLatestIndex());
        }
        else{
            int index = arg.prevLogIndex;
			int targetTerm = storage->getEntryByIndex(index).termNumber;                
            while(index - 1 >= storage->snapshot_index && storage->getEntryByIndex(index - 1).termNumber == targetTerm)
                index--;
			reply.conflictIndex = index;
            RAFT_LOG("I refuse %d's command.", arg.leaderId);
        }
        reply.success = false;
        return 0;
    }
    // RAFT_LOG("%d",arg.prevLogIndex);
    log_entry<command> prev = storage->back();
    while(prev.index > arg.prevLogIndex){
        storage->pop_back();
        prev = storage->back();
    }
    storage->pushBackEntrys(arg.entries);
    reply.success = true;
    RAFT_LOG("I accept %d's command.", arg.leaderId);
    
    if(role ==raft_role::follower && arg.leaderCommit > commitIndex){// If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
        commitIndex =  min(arg.leaderCommit, storage->currentLatestIndex());
        RAFT_LOG("I receive %d's message and renew my commitIndex to %d", arg.leaderId, commitIndex);
    }
    persistMetadata();

    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:

    if(reply.term > current_term && reply.success == false){
        std::lock_guard<std::mutex> guard(mtx);

        role = follower;
        current_term = reply.term;
        RAFT_LOG("term small. I am follower now.");
        persistMetadata();
        return;
    }
    if(arg.empty)
        return;
    
    if(reply.success == false && reply.term == current_term){
        std::lock_guard<std::mutex> guard(mtx);
        storage->setNextIndexs(target, reply.conflictIndex);
        RAFT_LOG("I send append entries to %d. It refuse. So I decrease its nextIndex to %d.", target, reply.conflictIndex);
        return;
    }
    if(reply.success == true && reply.term == current_term){
        std::lock_guard<std::mutex> guard(mtx);
        RAFT_LOG("I send append entries to %d. It accept. So I Renew its nextIndex to %d, its matchedIndex to %d.", target, (arg.entries).back().index + 1, (arg.entries).back().index);
        storage->setNextIndexs(target, (arg.entries).size() + storage->getNextIndexs(target));
        storage->setMatchedIndexs(target, storage->getNextIndexs(target) - 1);
        return;
    }
}

template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:

    std::lock_guard<std::mutex> guard(mtx);
    if(args.term > current_term){
        role = follower;
        current_term = args.term;
        persistMetadata();
    }
    reply.term = current_term;
    if(args.term < current_term){
        RAFT_LOG("refuse leader's snapshot");
        return 0;
    }
    
    renewRPCtime();

    // log.size() + snopshot_index;
    if(storage->currentLatestIndex() >= args.lastIncludedIndex //lastIncuded必须包含在现有log中
        && storage->snapshot_index < args.lastIncludedIndex //snapshot中含有尚未被本node提交的snapshot
        && storage->getEntryByIndex(args.lastIncludedIndex).termNumber == args.lastIncludedTerm 
        )
        storage->eraseBeforeLastIncluded(args.lastIncludedIndex);
    else
        storage->emptyLogs(args.lastIncludedIndex, args.lastIncludedTerm);
    
    storage->snapshot_index = args.lastIncludedIndex;
    lastApplied = args.lastIncludedIndex;
    commitIndex = args.lastIncludedIndex;
    storage->setSnapshotData(args.data);
    (static_cast<raft_state_machine*>(state))->apply_snapshot(args.data);
    RAFT_LOG("save snapshot");
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    std::lock_guard<std::mutex> guard(mtx);
    if(arg.term > current_term){
        role = follower;
        current_term = arg.term;
        RAFT_LOG("Renew my term to %d. I am follower now!", current_term);
    }
    storage->setNextIndexs(target, storage->snapshot_index + 1);
    storage->setMatchedIndexs(target, storage->snapshot_index);
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        timeval election;
	    gettimeofday(&election, NULL);
        long long election_start = election.tv_sec*1000 + election.tv_usec/1000;
        long long wait_time = election_start - last_RPC_time;
        long long timeout_ = randomTimeout();
        if (role != raft_role::leader &&(wait_time > timeout_ * 2)){
            RAFT_LOG("I begin an election! I have waited %lld, timeout: %lld", wait_time, timeout_*2);
            last_RPC_time = election_start;
            mtx.lock();
            current_term++;
            role = candidate;
            followCount = 1;
            persistMetadata();
            mtx.unlock();
            request_vote_args arg;
            arg.candidateId = my_id;
            arg.lastLogIndex = storage->back().index;
            arg.lastLogTerm = storage->back().termNumber;
            arg.term = current_term;

            for(int i = 0; i < (int)rpc_clients.size(); i++){
                if(i == my_id)
                    continue;

                send_request_vote(i, arg);
                if(role == raft_role::follower)
                    break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role == raft_role::leader){

            for(int target = 0; target < (int)rpc_clients.size(); target++){
                if(role != raft_role::leader) break;
                if(target == my_id) continue;

                if(storage->getNextIndexs(target) > storage->snapshot_index && storage->getNextIndexs(target) <= storage->currentLatestIndex()){
                    append_entries_args<command> arg;
                    arg.empty = false;
                    arg.leaderId = my_id;
                    arg.prevLogIndex = storage->getNextIndexs(target) - 1;

                    log_entry<command> prevEntry = storage->getEntryByIndex(arg.prevLogIndex);
                    arg.prevLogTerm = prevEntry.termNumber; 
                    
                    arg.term = current_term;
                    arg.entries = storage->entriesAfterIndex(storage->getNextIndexs(target));
                    arg.leaderCommit = commitIndex;
                    RAFT_LOG("I send append entries to %d,   It's current next index is %d, my current log index is %d, %d logs sent.", target, storage->getNextIndexs(target), storage->currentLatestIndex(), arg.entries.size());
                    if(role != raft_role::leader) break;
                    send_append_entries(target, arg);
                }
                if(storage->getNextIndexs(target) <= storage->snapshot_index){
                    RAFT_LOG("send snapshot to %d", target);
                    install_snapshot_args args;
                    args.term = current_term;
                    args.lastIncludedIndex = storage->snapshot_index;
                    args.lastIncludedTerm = storage->getEntryByIndex(storage->snapshot_index).termNumber;
                    args.data = storage->getSnapshotData();
                    send_install_snapshot(target, args);
                }
            }
        }

        if(role == raft_role::leader){
            bool canCommit = true;
            int mayBeCommited = commitIndex + 1;

            while(canCommit){
                int count = 0;
                for(int target = 0; target < (int)rpc_clients.size(); target++)
                    if(storage->getMatchedIndexs(target) >= mayBeCommited)
                        count++;
                if(count < (int)rpc_clients.size()/2+1)
                    canCommit = false;
                else
                    mayBeCommited++;
            }
            if(mayBeCommited - 1 != commitIndex)
                commitIndex = mayBeCommited - 1;
            RAFT_LOG("Renew commitIndex to %d", (mayBeCommited -1));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        while(commitIndex > lastApplied){
            RAFT_LOG("My lastApplied is %d, commitId is %d, so I begin commit", lastApplied, commitIndex);
            lastApplied ++;
            (static_cast<raft_state_machine*>(state))->apply_log((static_cast<raft_command&>(storage->getCommandByIndex(lastApplied))));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {    
    while (true) {
        if (is_stopped()) return;

        if(role == raft_role::leader)
            sendHeartBeat();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/



#endif // raft_h