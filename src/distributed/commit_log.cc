#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include "../include/block/manager.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */

CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
}

CommitLog::~CommitLog() {}


auto CommitLog::get_log_entry_num() -> usize {
  usize log_entry_num = 0;
  BlockManager *bm = this->bm_.get();
  std::vector<u16> log_super_block(bm->block_sz);
  memcpy(log_super_block.data(), bm->block_data + bm->block_cnt * bm->block_sz, bm->block_sz);
  auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
  u16 txn_entry_cnt = super_block->txn_entry_cnt;
  for (u16 i = 0; i < txn_entry_cnt; i++) {
    TxnEntry txn_entry;
    memcpy(&txn_entry, bm->block_data + (bm->block_cnt + LogBlockCntWoTxn) * bm->block_sz + i * sizeof(TxnEntry), sizeof(TxnEntry));
    if (txn_entry.is_committed != true) {
      log_entry_num++;
    }
  }
  printf("log_entry_num: %u\n", log_entry_num);
  return log_entry_num;
}


auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {

}

auto CommitLog::alloc_txn(TxnType txn_type, u8 type, inode_id_t parent,
                          const char *name) -> txn_id_t {
    BlockManager *bm = this->bm_.get();
    std::vector<u16> log_super_block(bm->block_sz);
    memcpy(log_super_block.data(), bm->block_data + bm->block_cnt * bm->block_sz, bm->block_sz);
    auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
    u16 current_txn_id = super_block->current_txn_id;
    u16 new_txn_id = current_txn_id + 1;
    u16 txn_entry_cnt = super_block->txn_entry_cnt;
    if (txn_entry_cnt >= maxTxnEntryCnt) {
        checkpoint();
    }
    memcpy(log_super_block.data(), bm->block_data + bm->block_cnt * bm->block_sz, bm->block_sz);
    super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
    current_txn_id = super_block->current_txn_id;
    txn_entry_cnt = super_block->txn_entry_cnt;
    TxnEntry txn_entry = TxnEntry(new_txn_id, txn_type, type, parent, name);
    memcpy(bm->block_data + (bm->block_cnt + LogBlockCntWoTxn) * bm->block_sz + txn_entry_cnt * sizeof(TxnEntry), &txn_entry, sizeof(TxnEntry));
    bm->sync(bm->block_cnt + LogBlockCntWoTxn);
    super_block->current_txn_id = new_txn_id;
    super_block->txn_entry_cnt = txn_entry_cnt + 1;
    memcpy(bm->block_data + bm->block_cnt * bm->block_sz, log_super_block.data(), bm->block_sz);
    bm->sync(bm->block_cnt);
    return new_txn_id;
}


auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  BlockManager *bm = this->bm_.get();
  std::vector<u16> log_super_block(bm->block_sz);
  memcpy(log_super_block.data(), bm->block_data + bm->block_cnt * bm->block_sz, bm->block_sz);
  auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
  u16 txn_entry_cnt = super_block->txn_entry_cnt;
  for (u16 i = 0; i < txn_entry_cnt; i++) {
    TxnEntry txn_entry;
    memcpy(&txn_entry, bm->block_data + (bm->block_cnt + LogBlockCntWoTxn) * bm->block_sz + i * sizeof(TxnEntry), sizeof(TxnEntry));
    if (txn_entry.txn_id == txn_id) {
      txn_entry.is_committed = true;
        if (txn_entry.table_offset == -1) {
          break;
        } else {
          int64_t current_table_offset = txn_entry.table_offset;
          while (true) {
            ActionEntry action_entry;
            memcpy(&action_entry, bm->block_data + (bm->block_cnt + 1) * bm->block_sz + current_table_offset, sizeof(ActionEntry));
            block_id_t block_id = action_entry.block_id;
            u16 current_log_id = current_table_offset / sizeof(ActionEntry);
            std::vector<u8> log_block(bm->block_sz);
            memcpy(log_block.data(), bm->block_data + (bm->block_cnt + LogPrefixBlockCnt + current_log_id) * bm->block_sz, bm->block_sz);
            memcpy(bm->block_data + block_id * bm->block_sz, log_block.data(), bm->block_sz);
            bm->sync(block_id);
            if (action_entry.table_offset == -1) {
              break;
            }
            current_table_offset = action_entry.table_offset;
          }
        }
      memcpy(bm->block_data + (bm->block_cnt + LogBlockCntWoTxn) * bm->block_sz + i * sizeof(TxnEntry), &txn_entry, sizeof(TxnEntry));
      bm->sync(bm->block_cnt + LogBlockCntWoTxn);
      break;
    }
  }
}


auto CommitLog::checkpoint() -> void {
  printf("checkpoint\n");
  BlockManager *bm = this->bm_.get();
  std::vector<u16> log_super_block(bm->block_sz);
  memcpy(log_super_block.data(), bm->block_data + bm->block_cnt * bm->block_sz, bm->block_sz);
  auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
  u16 txn_entry_cnt = super_block->txn_entry_cnt;
  std::vector<TxnEntry> txn_entries;
  u16 new_txn_entry_cnt = 0;
  for (u16 i = 0; i < txn_entry_cnt; i++) {
    TxnEntry txn_entry;
    memcpy(&txn_entry, bm->block_data + (bm->block_cnt + LogBlockCntWoTxn) * bm->block_sz + i * sizeof(TxnEntry), sizeof(TxnEntry));
    if (txn_entry.is_committed != true) {
      txn_entries.push_back(txn_entry);
      new_txn_entry_cnt++;
    }
  }
  for (u16 i = 0; i < new_txn_entry_cnt; i++) {
    memcpy(bm->block_data + (bm->block_cnt + LogBlockCntWoTxn) * bm->block_sz + i * sizeof(TxnEntry), &txn_entries[i], sizeof(TxnEntry));
  }
  super_block->txn_entry_cnt = new_txn_entry_cnt;
  memcpy(bm->block_data + bm->block_cnt * bm->block_sz, log_super_block.data(), bm->block_sz);
  bm->sync(bm->block_cnt);
}


auto CommitLog::recover() -> void {

}
}; // namespace chfs