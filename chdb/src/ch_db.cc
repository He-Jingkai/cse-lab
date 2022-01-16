#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc,
                         const chdb_protocol::operation_var &var, int &r) {
// TODO: Your code here
#if BIG_LOCK
  if (proc == chdb_protocol::Get || proc == chdb_protocol::Put) {
    if (var.tx_id == tx_holdLock)
      goto excute;
    else {
      lock_.lock();
      tx_holdLock = var.tx_id;
    }
  }
#endif

excute:
#if RAFT_GROUP
  /* 测试part2时将此部分取消注释！ */
  // if (proc == chdb_protocol::Dummy || proc == chdb_protocol::Get ||
  //     proc == chdb_protocol::Put) {
  //   chdb_command::command_type cmd_type;
  //   switch (proc) {
  //     case chdb_protocol::rpc_numbers::Dummy:
  //       cmd_type = chdb_command::command_type::CMD_NONE;
  //       break;
  //     case chdb_protocol::rpc_numbers::Put:
  //       cmd_type = chdb_command::command_type::CMD_PUT;
  //       break;
  //     case chdb_protocol::rpc_numbers::Get:
  //       cmd_type = chdb_command::command_type::CMD_GET;
  //       break;
  //     default:
  //       break;
  //   }
  //   chdb_command cmd(cmd_type, var.key, var.value, var.tx_id);
  //   int term, index;
  //   this->leader()->new_command(cmd, term, index);
  //     while (!cmd.res->done)
  //       std::this_thread::sleep_for(std::chrono::milliseconds(50));
  // }
#endif
  int base_port = this->node->port();
  int shard_offset = this->dispatch(query_key, shard_num());

  int ret = this->node->template call(base_port + shard_offset, proc, var, r);

#if BIG_LOCK
  if (proc == chdb_protocol::Commit || proc == chdb_protocol::Rollback)
    if (var.tx_id == tx_holdLock) {
      tx_holdLock = -1;
      lock_.unlock();
    }
#endif
  return r;
}

view_server::~view_server() {
#if RAFT_GROUP
  delete this->raft_group;
#endif
  delete this->node;
}