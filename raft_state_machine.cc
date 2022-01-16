#include "raft_state_machine.h"
#include "fstream"
#include "iostream"
#include <sstream>

kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) {
        cmd_tp = (cmd.cmd_tp);
        key = (cmd.key);
        value = (cmd.value);
        res = (cmd.res);
    }

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return 150;
}


// void kv_command::serialize(char* buf, int size) const {
//     // Your code here:
//     sprintf(buf, "type:%d key:%s value:%s", (int)cmd_tp, key.c_str(), value.c_str());
// }

// void kv_command::deserialize(const char* buf, int size) {
//     // Your code here:
//     char* key_buf = new char[100];
//     char* value_buf = new char[100];
    
//     sscanf(buf, "type:%d key:%s value:%s", (int*)(&cmd_tp), key_buf, value_buf);
//     key = key_buf;
//     value = value_buf;
// }
void kv_command::serialize(char *buf, int size) const {
    // Your code here:
    sprintf(buf, "type:%d key:%s value:%s", (int) cmd_tp, key.c_str(), value.c_str());
}

void kv_command::deserialize(const char *buf, int size) {
    // Your code here:
    char key_buf[100], val_buf[100];
    sscanf(buf, "type:%d key:%s value:%s", (int *) &cmd_tp, key_buf, val_buf);
    key = key_buf;
    value = val_buf;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m<<(int)cmd.cmd_tp<<cmd.key<<cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    u>>(int&)cmd.cmd_tp>>cmd.key>>cmd.value;
    return u;
}

std::ofstream& operator<<(std::ofstream &m, const kv_command& cmd){
    // Your code here:
    m<<cmd.cmd_tp<<"\t"<<cmd.key<<"\t"<<cmd.value<<"\t";
    return m;
}

std::ifstream& operator>>(std::ifstream &u, kv_command& cmd){
    // Your code here:
    u >> (int&)cmd.cmd_tp;
    if(cmd.cmd_tp == 0)return u;
    if(cmd.cmd_tp!=2){
        u>>cmd.key;
        cmd.value = "";
    }
    else
        u>>cmd.key>>cmd.value;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    switch (kv_cmd.cmd_tp)
    {
    case 0:
        break;
    case 1:
        if(kv_table.find(kv_cmd.key)==kv_table.end()){
            kv_cmd.res->succ = false;
            kv_cmd.res->value = "";
        }
        else{
            kv_cmd.res->value = kv_table.find(kv_cmd.key)->second;
            kv_cmd.res->succ = true;
        }
        break;
    case 2:
        kv_table[kv_cmd.key] = kv_cmd.value;
        kv_cmd.res->value = kv_cmd.value;
        kv_cmd.res->succ = true;
        break;
    case 3:
        if(kv_table.find(kv_cmd.key)==kv_table.end()){
            kv_cmd.res->succ = false;
            kv_cmd.res->value = "";
        }
        else{
            kv_cmd.res->value = kv_table.find(kv_cmd.key)->second;
            kv_table.erase(kv_cmd.key);
            kv_cmd.res->succ = true;
        }
    
    default:
        break;
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::vector<char> data;
    std::stringstream ss;
    ss << (int)kv_table.size();
    for (auto kv : kv_table)
        ss << ' ' << kv.first << ' ' << kv.second ;
    std::string str = ss.str();
    data.assign(str.begin(), str.end());
    return data;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    std::string str;
    str.assign(snapshot.begin(), snapshot.end());
    std::stringstream ss(str);
    kv_table = std::map<std::string, std::string>();
    int size;
    ss >> size;
    for (int i = 0; i < size; i++) {
        std::string first;
        std::string second;
        ss >>  first >> second;
        kv_table[first] = second;
    }
}
