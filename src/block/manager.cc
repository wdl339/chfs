#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "block/manager.h"

namespace chfs {

auto get_file_sz(std::string &file_name) -> usize {
  std::filesystem::path path = file_name;
  return std::filesystem::file_size(path);
}

/**
 * If the opened block manager's backed file is empty,
 * we will initialize it to a pre-defined size.
 */
auto initialize_file(int fd, u64 total_file_sz) {
  if (ftruncate(fd, total_file_sz) == -1) {
    CHFS_ASSERT(false, "Failed to initialize the block manager file");
  }
}

/**
 * Constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
BlockManager::BlockManager(const std::string &file)
    : BlockManager(file, KDefaultBlockCnt) {}

/**
 * Creates a new block manager that writes to a file-backed block device.
 * @param block_file the file name of the  file to write to
 * @param block_cnt the number of expected blocks in the device. If the
 * device's blocks are more or less than it, the manager should adjust the
 * actual block cnt.
 */
BlockManager::BlockManager(usize block_cnt, usize block_size)
    : block_sz(block_size), file_name_("in-memory"), fd(-1),
      block_cnt(block_cnt), in_memory(true) {
  // An important step to prevent overflow
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  this->is_log_enabled = false;
  u64 buf_sz = static_cast<u64>(block_cnt) * static_cast<u64>(block_size);
  CHFS_VERIFY(buf_sz > 0, "Santiy check buffer size fails");
  this->block_data = new u8[buf_sz];
  CHFS_VERIFY(this->block_data != nullptr, "Failed to allocate memory");
}

/**
 * Core constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
BlockManager::BlockManager(const std::string &file, usize block_cnt)
    : file_name_(file), block_cnt(block_cnt), in_memory(false) {
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  this->is_log_enabled = false;
  this->fd = open(file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  CHFS_ASSERT(this->fd != -1, "Failed to open the block manager file");

  auto file_sz = get_file_sz(this->file_name_);
  auto log_sz = this->block_sz * LogBlockCnt;
  if (file_sz == 0 || file_sz == this->total_storage_sz() + log_sz) {
    initialize_file(this->fd, this->total_storage_sz());
  } else {
    this->block_cnt = file_sz / this->block_sz;
    CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                "The file size mismatches");
  }

  this->block_data =
      static_cast<u8 *>(mmap(nullptr, this->total_storage_sz(),
                             PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
  CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data");
}

BlockManager::BlockManager(const std::string &file, usize block_cnt, bool is_log_enabled)
    : file_name_(file), block_cnt(block_cnt), in_memory(false) {
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  this->is_log_enabled = is_log_enabled;
  this->fd = open(file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  CHFS_ASSERT(this->fd != -1, "Failed to open the block manager file");

  auto file_sz = get_file_sz(this->file_name_);
  auto log_sz = this->block_sz * LogBlockCnt;
  if (is_log_enabled){
    // printf("Log enabled\n");
    if (file_sz == 0 || file_sz == this->total_storage_sz()) {
      initialize_file(this->fd, this->total_storage_sz() + log_sz);
    } else {
      this->block_cnt = (file_sz - log_sz) / this->block_sz;
      CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                  "The file size mismatches");
    }

    this->block_data =
      static_cast<u8 *>(mmap(nullptr, this->total_storage_sz() + log_sz,
                             PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
    CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data"); 
  } else {
    // printf("Log not enabled\n");
    if (file_sz == 0 || file_sz == this->total_storage_sz() + log_sz) {
      initialize_file(this->fd, this->total_storage_sz());
    } else {
      this->block_cnt = file_sz / this->block_sz;
      CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                  "The file size mismatches");
    }

    this->block_data =
        static_cast<u8 *>(mmap(nullptr, this->total_storage_sz(),
                              PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
    CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data");
  }   
}

auto BlockManager::write_block(block_id_t block_id, const u8 *data)
    -> ChfsNullResult {
  if (this->maybe_failed && block_id < this->block_cnt) {
    if (this->write_fail_cnt >= 3) {
      this->write_fail_cnt = 0;
      return ErrorType::INVALID;
    }
  }

  // printf("Writing block %lu\n", block_id);
  // printf("writing data: %s\n", data);

  memcpy(this->block_data + block_id * this->block_sz, data, this->block_sz);

  if (this->is_log_enabled){
    std::vector<u16> log_super_block(this->block_sz);
    memcpy(log_super_block.data(), this->block_data + this->block_cnt * this->block_sz, this->block_sz);
    auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
    u16 current_log_id = super_block->current_log_id;
    u16 log_block_cnt = super_block->log_block_cnt;
    u16 current_txn_id = super_block->current_txn_id;
    u16 txn_entry_cnt = super_block->txn_entry_cnt;
    int64_t table_offset = current_log_id * sizeof(ActionEntry);

    for (u16 i = 0; i < txn_entry_cnt; i++) {
      TxnEntry txn_entry;
      memcpy(&txn_entry, this->block_data + (this->block_cnt + LogBlockCntWoTxn) * this->block_sz + i * sizeof(TxnEntry), sizeof(TxnEntry));
      if (txn_entry.txn_id == current_txn_id) {
        if (txn_entry.table_offset == -1) {
          txn_entry.table_offset = table_offset;
          memcpy(this->block_data + (this->block_cnt + LogBlockCntWoTxn) * this->block_sz + i * sizeof(TxnEntry), &txn_entry, sizeof(TxnEntry));
          this->sync(this->block_cnt + LogBlockCntWoTxn);
          ActionEntry action_entry;
          action_entry.block_id = block_id;
          action_entry.table_offset = -1;
          memcpy(this->block_data + (this->block_cnt + 1) * this->block_sz + table_offset, &action_entry, sizeof(ActionEntry));
          this->sync(this->block_cnt + 1 + table_offset / DiskBlockSize);
          break;
        } else {
          int64_t current_table_offset = txn_entry.table_offset;
          while (true) {
            ActionEntry action_entry;
            memcpy(&action_entry, this->block_data + (this->block_cnt + 1) * this->block_sz + current_table_offset, sizeof(ActionEntry));
            if (action_entry.table_offset == -1) {
              action_entry.table_offset = table_offset;
              ActionEntry new_action_entry;
              new_action_entry.block_id = block_id;
              new_action_entry.table_offset = -1;
              memcpy(this->block_data + (this->block_cnt + 1) * this->block_sz + current_table_offset, &action_entry, sizeof(ActionEntry));
              memcpy(this->block_data + (this->block_cnt + 1) * this->block_sz + table_offset, &new_action_entry, sizeof(ActionEntry));
              for(int i = 0; i < LogTableBlockCnt; i++){
                this->sync(this->block_cnt + 1 + i);
              }
              break;
            }
            current_table_offset = action_entry.table_offset;
          }
        }
        break;
      }
    }

    memcpy(this->block_data + (this->block_cnt + LogPrefixBlockCnt + current_log_id) * this->block_sz, data, this->block_sz);
    this->sync(this->block_cnt + LogPrefixBlockCnt + current_log_id);

    super_block->current_log_id = (current_log_id + 1) % RealLogBlockCnt;
    super_block->log_block_cnt = log_block_cnt + 1;
    memcpy(this->block_data + this->block_cnt * this->block_sz, log_super_block.data(), this->block_sz);
    this->sync(this->block_cnt);
  }

  this->write_fail_cnt++;
  return KNullOk;
}

auto BlockManager::write_partial_block(block_id_t block_id, const u8 *data,
                                       usize offset, usize len)
    -> ChfsNullResult {
  if (this->maybe_failed && block_id < this->block_cnt) {
    if (this->write_fail_cnt >= 3) {
      this->write_fail_cnt = 0;
      printf("Want to write %s to block %lu but fail\n", data, block_id);
      return ErrorType::INVALID;
    }
  }

  // printf("Writing block %lu\n", block_id);
  // printf("writing data: %s\n", data);

  memcpy(this->block_data + block_id * this->block_sz + offset, data, len);

  if (this->is_log_enabled){
    std::vector<u16> log_super_block(this->block_sz);
    memcpy(log_super_block.data(), this->block_data + this->block_cnt * this->block_sz, this->block_sz);
    auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
    u16 current_log_id = super_block->current_log_id;
    u16 log_block_cnt = super_block->log_block_cnt;
    u16 current_txn_id = super_block->current_txn_id;
    u16 txn_entry_cnt = super_block->txn_entry_cnt;
    int64_t table_offset = current_log_id * sizeof(ActionEntry);

    for (u16 i = 0; i < txn_entry_cnt; i++) {
      TxnEntry txn_entry;
      memcpy(&txn_entry, this->block_data + (this->block_cnt + LogBlockCntWoTxn) * this->block_sz + i * sizeof(TxnEntry), sizeof(TxnEntry));
      if (txn_entry.txn_id == current_txn_id) {
        if (txn_entry.table_offset == -1) {
          txn_entry.table_offset = table_offset;
          memcpy(this->block_data + (this->block_cnt + LogBlockCntWoTxn) * this->block_sz + i * sizeof(TxnEntry), &txn_entry, sizeof(TxnEntry));
          this->sync(this->block_cnt + LogBlockCntWoTxn);
          ActionEntry action_entry;
          action_entry.block_id = block_id;
          action_entry.table_offset = -1;
          memcpy(this->block_data + (this->block_cnt + 1) * this->block_sz + table_offset, &action_entry, sizeof(ActionEntry));
          this->sync(this->block_cnt + 1 + table_offset / DiskBlockSize);
          break;
        } else {
          int64_t current_table_offset = txn_entry.table_offset;
          while (true) {
            ActionEntry action_entry;
            memcpy(&action_entry, this->block_data + (this->block_cnt + 1) * this->block_sz + current_table_offset, sizeof(ActionEntry));
            if (action_entry.table_offset == -1) {
              action_entry.table_offset = table_offset;
              ActionEntry new_action_entry;
              new_action_entry.block_id = block_id;
              new_action_entry.table_offset = -1;
              memcpy(this->block_data + (this->block_cnt + 1) * this->block_sz + current_table_offset, &action_entry, sizeof(ActionEntry));
              memcpy(this->block_data + (this->block_cnt + 1) * this->block_sz + table_offset, &new_action_entry, sizeof(ActionEntry));
              for(int i = 0; i < LogTableBlockCnt; i++){
                this->sync(this->block_cnt + 1 + i);
              }
              break;
            }
            current_table_offset = action_entry.table_offset;
          }
        }
        break;
      }
    }

    std::vector<u8> log_block(this->block_sz);
    memcpy(log_block.data(), this->block_data + block_id * this->block_sz, this->block_sz);
    memcpy(this->block_data + (this->block_cnt + LogPrefixBlockCnt + current_log_id) * this->block_sz, log_block.data(), this->block_sz);
    this->sync(this->block_cnt + LogPrefixBlockCnt + current_log_id);

    super_block->current_log_id = (current_log_id + 1) % RealLogBlockCnt;
    super_block->log_block_cnt = log_block_cnt + 1;
    memcpy(this->block_data + this->block_cnt * this->block_sz, log_super_block.data(), this->block_sz);
    this->sync(this->block_cnt);
  }

  this->write_fail_cnt++;
  return KNullOk;
}

// void BlockManager::recover() {
//   if (!this->is_log_enabled){
//     return;
//   }
//   // printf("Recovering\n");
//   printf("sizeof TxnEntry: %lu\n", sizeof(TxnEntry));
//   printf("sizeof ActionEntry: %lu\n", sizeof(ActionEntry));

//   std::vector<u16> log_super_block(this->block_sz / 2);
//   memcpy(log_super_block.data(), this->block_data + this->block_cnt * this->block_sz, this->block_sz);
//   auto super_block = reinterpret_cast<SuperLogBlock *>(log_super_block.data());
//   u16 current_log_id = super_block->current_log_id;
//   // printf("Current log id: %d\n", current_log_id);

//   if (current_log_id == 0){
//     return;
//   }

//   auto table_cnt = (current_log_id - 1) / LogTableEntryCnt;
//   for (u8 i = 0; i <= table_cnt; i++){
//     auto entry_cnt = i == table_cnt ? (current_log_id - 1) % LogTableEntryCnt : LogTableEntryCnt;
//     std::vector<u8> log_table(this->block_sz);
//     memcpy(log_table.data(), this->block_data + (this->block_cnt + 1 + i) * this->block_sz, this->block_sz);
//     for (u8 j = 0; j <= entry_cnt; j++){
//       block_id_t block_id = log_table[j];
//       // printf("Recovering block %lu\n", block_id);
//       std::vector<u8> log_block(this->block_sz);
//       memcpy(log_block.data(), this->block_data + (this->block_cnt + LogPrefixBlockCnt + i * LogTableEntryCnt + j) * this->block_sz, this->block_sz);
//       // printf("Data: %s\n", log_block.data());
//       memcpy(this->block_data + block_id * this->block_sz, log_block.data(), this->block_sz);
//       this->sync(block_id);
//     }
//   }

//   memset(this->block_data + this->block_cnt * this->block_sz, 0, this->block_sz * LogPrefixBlockCnt);
//   for (u8 i = 0; i < LogPrefixBlockCnt; i++){
//     this->sync(this->block_cnt + i);
//   }
  
// }

auto BlockManager::read_block(block_id_t block_id, u8 *data) -> ChfsNullResult {
  memcpy(data, this->block_data + block_id * this->block_sz, this->block_sz);
  return KNullOk;
}

auto BlockManager::zero_block(block_id_t block_id) -> ChfsNullResult {
  memset(this->block_data + block_id * this->block_sz, 0, this->block_sz);
  return KNullOk;
}

auto BlockManager::sync(block_id_t block_id) -> ChfsNullResult {
  if (block_id >= this->block_cnt + LogBlockCnt) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  auto res = msync(this->block_data + block_id * this->block_sz, this->block_sz,
        MS_SYNC | MS_INVALIDATE);
  if (res != 0)
    return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

auto BlockManager::flush() -> ChfsNullResult {
  auto res = msync(this->block_data, this->block_sz * this->block_cnt, MS_SYNC | MS_INVALIDATE);
  if (res != 0)
    return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

BlockManager::~BlockManager() {
  if (!this->in_memory) {
    munmap(this->block_data, this->total_storage_sz());
    close(this->fd);
  } else {
    delete[] this->block_data;
  }
}

// BlockIterator
auto BlockIterator::create(BlockManager *bm, block_id_t start_block_id,
                           block_id_t end_block_id)
    -> ChfsResult<BlockIterator> {
  BlockIterator iter;
  iter.bm = bm;
  iter.cur_block_off = 0;
  iter.start_block_id = start_block_id;
  iter.end_block_id = end_block_id;

  std::vector<u8> buffer(bm->block_sz);

  auto res = bm->read_block(iter.cur_block_off / bm->block_sz + start_block_id,
                            buffer.data());
  if (res.is_ok()) {
    iter.buffer = std::move(buffer);
    return ChfsResult<BlockIterator>(iter);
  }
  return ChfsResult<BlockIterator>(res.unwrap_error());
}

// assumption: a previous call of has_next() returns true
auto BlockIterator::next(usize offset) -> ChfsNullResult {
  auto prev_block_id = this->cur_block_off / bm->block_size();
  this->cur_block_off += offset;

  auto new_block_id = this->cur_block_off / bm->block_size();
  // move forward
  if (new_block_id != prev_block_id) {
    if (this->start_block_id + new_block_id > this->end_block_id) {
      return ChfsNullResult(ErrorType::DONE);
    }

    // else: we need to refresh the buffer
    auto res = bm->read_block(this->start_block_id + new_block_id,
                              this->buffer.data());
    if (res.is_err()) {
      return ChfsNullResult(res.unwrap_error());
    }
  }
  return KNullOk;
}

} // namespace chfs
