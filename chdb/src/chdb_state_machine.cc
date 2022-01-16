#include "chdb_state_machine.h"

#include "fstream"

chdb_command::chdb_command():chdb_command(CMD_NONE, 0, 0, 0) {
  // TODO: Your code here
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value,
                           const int &tx_id)
    : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()) {
  // TODO: Your code here
}

chdb_command::chdb_command(const chdb_command &cmd)
    : cmd_tp(cmd.cmd_tp),
      key(cmd.key),
      value(cmd.value),
      tx_id(cmd.tx_id),
      res(cmd.res) {
  // TODO: Your code here
}

void chdb_command::serialize(char *buf, int size) const {
  // TODO: Your code here
}

void chdb_command::deserialize(const char *buf, int size) {
  // TODO: Your code here
}

std::ofstream &operator<<(std::ofstream &m, const chdb_command &cmd) {
  // Your code here:
  m << cmd.cmd_tp << "\t" << cmd.key << "\t" << cmd.value << "\t" << cmd.tx_id;
  return m;
}

std::ifstream &operator>>(std::ifstream &u, chdb_command &cmd) {
  // Your code here:
  u >> (int &)cmd.cmd_tp >> cmd.key >> cmd.value >> (int &)cmd.tx_id;
  return u;
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
  // TODO: Your code here
  return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
  // TODO: Your code here
  return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
  // TODO: Your code here
  chdb_command &kv_cmd = dynamic_cast<chdb_command &>(cmd);
  std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
  // Your code here:
  switch (kv_cmd.cmd_tp) {
    case chdb_command::command_type::CMD_NONE:
      break;
    case chdb_command::command_type::CMD_GET:
      if (kv_table.find(kv_cmd.key) == kv_table.end()) {
        kv_cmd.res->value = 0;
      } else {
        kv_cmd.res->value = kv_table.find(kv_cmd.key)->second;
      }
      break;
    case chdb_command::command_type::CMD_PUT:
      kv_table[kv_cmd.key] = kv_cmd.value;
      kv_cmd.res->value = kv_cmd.value;
      break;
    default:
      break;
  }
  kv_cmd.res->done = true;
  kv_cmd.res->cv.notify_all();
  return;
}