#include "tx_region.h"

int tx_region::put(const int key, const int val) {
  // TODO: Your code here
  int r, rt;
#if !BIG_LOCK
  acquireLock(key);
#endif
  this->db->vserver->execute(
      key, chdb_protocol::Put,
      chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val},
      r);
  queryKeys.push_back(key);
  return r;
}

int tx_region::get(const int key) {
  // TODO: Your code here
  int r, rt;
#if !BIG_LOCK
  acquireLock(key);
#endif
  this->db->vserver->execute(
      key, chdb_protocol::Get,
      chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0}, r);
  queryKeysREAD.push_back(key);
  return r;
}

int tx_region::tx_can_commit() {
  // TODO: Your code here
  if (!queryKeys.size()) return chdb_protocol::prepare_ok;
  for (int queryKey : queryKeys) {
    int r;
    this->db->vserver->execute(
        queryKey, chdb_protocol::CheckPrepareState,
        chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0}, r);
    if (r == (int)false) return chdb_protocol::prepare_not_ok;
  }
  return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
  // TODO: Your code here
  printf("tx[%d] begin\n", tx_id);
  return 0;
}

int tx_region::tx_commit() {
  // TODO: Your code here
  for (int queryKey : queryKeys) {
    int r;
    this->db->vserver->execute(
        queryKey, chdb_protocol::Commit,
        chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0}, r);
  }
  for (int queryKey : queryKeysREAD) {
    int r;
    this->db->vserver->execute(
        queryKey, chdb_protocol::Commit,
        chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0}, r);
  }
  printf("tx[%d] commit\n", tx_id);
  releseLock();
  return 0;
}

int tx_region::tx_abort() {
  // TODO: Your code here
  for (int queryKey : queryKeys) {
    int r;
    this->db->vserver->execute(
        queryKey, chdb_protocol::Rollback,
        chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0}, r);
  }

  printf("tx[%d] abort\n", tx_id);
  releseLock();
  return 0;
}

void tx_region::acquireLock(int key) {
  if (std::find(queryKeys.begin(), queryKeys.end(), key) != queryKeys.end() ||
      std::find(queryKeysREAD.begin(), queryKeysREAD.end(), key) !=
          queryKeysREAD.end())
    return;
  while (!this->db->acquireLock(key, tx_id));
}

void tx_region::releseLock() {
  for (int readKey : queryKeysREAD) this->db->releaseLock(readKey, tx_id);
  for (int putKey : queryKeys)
    if (std::find(queryKeysREAD.begin(), queryKeysREAD.end(), putKey) ==
        queryKeysREAD.end())
      this->db->releaseLock(putKey, tx_id);
}
