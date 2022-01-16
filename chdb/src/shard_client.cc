#include "shard_client.h"

#include <iostream>

int shard_client::put(chdb_protocol::operation_var var, int &r) {
  // TODO: Your code here
  log[var.tx_id].push_back(std::pair<int, int>(var.key, var.value));
  r = var.value;
  return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
  // TODO: Your code here
  for (std::pair<int, int> kv : log[var.tx_id])
    if (kv.first == var.key) {
      r = kv.second;
      return 0;
    }
  r = store[primary_replica][var.key].value;
  return 0;
}

int shard_client::commit(chdb_protocol::operation_var var, int &r) {
  // TODO: Your code here
  for (std::pair<int, int> kv : log[var.tx_id])
    for (int i = 0; i < replica_num; i++)
      store[i][kv.first] = value_entry(kv.second);

  return 0;
}

int shard_client::rollback(chdb_protocol::operation_var var, int &r) {
  // TODO: Your code here
  log[var.tx_id].empty();
  return 0;
}

int shard_client::check_prepare_state(chdb_protocol::operation_var var,
                                      int &r) {
  // TODO: Your code here
  r = (int)this->active;
  return 0;
}

int shard_client::prepare(chdb_protocol::operation_var var, int &r) {
  // TODO: Your code here
  return 0;
}

shard_client::~shard_client() { delete node; }
