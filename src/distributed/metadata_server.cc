#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}


auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  const char* name_c = name.c_str();
  fo_mtx.lock();
  // auto res = this->operation_->mk_helper(parent, name_c, static_cast<InodeType>(type));
  // fo_mtx.unlock();
  txn_id_t txn_id ;

  if (is_log_enabled_) {
    txn_id = commit_log->alloc_txn(TxnType::MKNODE, type, parent, name_c);
  }

  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this->operation_.get(), parent, list);
  if (read_res.is_err()) {
    fo_mtx.unlock();
    return 0;
  }

  for (const auto &entry : list) {
    if (entry.name == name) {
      fo_mtx.unlock();
      return 0;
    }
  }

  auto alloc_res = this->operation_->alloc_inode(static_cast<InodeType>(type));
  if (alloc_res.is_err()) {
    fo_mtx.unlock();
    return 0;
  }
  inode_id_t new_inode_id = alloc_res.unwrap();

  std::string dir_content = dir_list_to_string(list);
  if (!dir_content.empty()){
    dir_content += "/";
  }
  dir_content = append_to_directory(dir_content, name_c, new_inode_id);
  std::vector<u8> data(dir_content.begin(), dir_content.end());

  auto write_res = this->operation_->write_file(parent, data);
  if (write_res.is_err()) {
    fo_mtx.unlock();
    return 0;
  }

  if (is_log_enabled_) {
    commit_log->commit_log(txn_id);
  }

  fo_mtx.unlock();
  return new_inode_id;
}


auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  const char* name_c = name.c_str();
  fo_mtx.lock();
  // auto res = this->operation_->unlink(parent, name_c);
  // fo_mtx.unlock();

  txn_id_t txn_id;
  if (is_log_enabled_) {
    txn_id = commit_log->alloc_txn(TxnType::UNLINK, 0, parent, name_c);
  }

  std::list<DirectoryEntry> list;
  list.clear();
  auto read_res = read_directory(this->operation_.get(), parent, list);
  if (read_res.is_err()) {
    fo_mtx.unlock();
    return false;
  }

  auto lookup_res = this->operation_->lookup(parent, name_c);
  if (lookup_res.is_err()) {
    fo_mtx.unlock();
    return false;
  }
  inode_id_t inode_id = lookup_res.unwrap();

  auto remove_res = this->operation_->remove_file(inode_id);
  if (remove_res.is_err()) {
    fo_mtx.unlock();
    return false;
  }

  std::string dir_content = dir_list_to_string(list);
  if (!dir_content.empty()){
    dir_content += "/";
  }
  dir_content = rm_from_directory(dir_content, name);
  std::vector<u8> data(dir_content.begin(), dir_content.end());

  auto write_res = this->operation_->write_file(parent, data);
  if (write_res.is_err()) {
    fo_mtx.unlock();
    return false;
  }

  if (is_log_enabled_) {
    commit_log->commit_log(txn_id);
  }
  
  fo_mtx.unlock();
  return true;
}


auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  const char* name_c = name.c_str();
  auto res = this->operation_->lookup(parent, name_c);

  if (res.is_err()) {
    return 0;
  }

  return res.unwrap();
}


auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  std::vector<BlockInfo> ret;
  usize block_size = this->operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  fo_mtx.lock();
  auto inode_res = this->operation_->inode_manager_->read_inode(id, inode);
  fo_mtx.unlock();
  if (inode_res.is_err()) {
    return {};
  }

  u64 content_sz = inode_p->get_size();
  auto num_block = content_sz / block_size;
  if(content_sz % block_size != 0)
    num_block++;

  for(int i = 0; i < num_block; i++){
    block_id_t block_id = inode_p->blocks[i * 2];
    u64 mv_id = inode_p->blocks[i * 2 + 1];
    mac_id_t mac_id = mv_id >> 32;
    version_t version_id = mv_id & 0xFFFFFFFF;
    ret.push_back({block_id, mac_id, version_id});
  }

  return ret;
}


auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  auto bm = this->operation_->block_manager_;
  usize block_size = bm->block_size();
  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  fo_mtx.lock();
  auto read_res = this->operation_->inode_manager_->read_inode(id, inode);
  if (read_res.is_err()) {
    return {};
  }
  auto inode_id = read_res.unwrap();
  
  block_id_t block_id = 0;
  mac_id_t mac_id = 0;
  version_t version_id = 0;
  mac_id_t generated_id = generator.rand(1, num_data_servers);

  for(int try_times = 0; ; try_times++){
    mac_id = (generated_id + try_times) % num_data_servers + 1;
    auto alloc_res = clients_[mac_id]->call("alloc_block");
    if(alloc_res.is_err())
      continue;

    auto resp = alloc_res.unwrap();
    auto bv_id = resp->as<std::pair<block_id_t, version_t>>();
    block_id = bv_id.first;
    version_id = bv_id.second;
    if(!block_id)
      continue;
      
    break;
  }

  u64 content_sz = inode_p->get_size();
  auto num_block = content_sz / block_size;
  if(content_sz % block_size != 0)
    num_block++;

  inode_p->blocks[num_block * 2] = block_id;
  inode_p->blocks[num_block * 2 + 1] = (static_cast<u64>(mac_id) << 32) | static_cast<u64>(version_id);
  inode_p->inner_attr.size += block_size;
  inode_p->inner_attr.set_all_time(time(0));

  auto write_res = bm->write_block(inode_id, inode.data());
  fo_mtx.unlock();
  if (write_res.is_err()) {
    return {};
  }

  return {block_id, mac_id, version_id};
}


auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  auto bm = this->operation_->block_manager_;
  usize block_size = bm->block_size();
  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  fo_mtx.lock();
  auto read_res = this->operation_->inode_manager_->read_inode(id, inode);
  if (read_res.is_err()) {
    return false;
  }
  auto inode_id = read_res.unwrap();

  u64 content_sz = inode_p->get_size();
  auto num_block = content_sz / block_size;
  if(content_sz % block_size != 0)
    num_block++;

  bool find = false;

  for(int i = 0; i < num_block; i++){
    if (!find){
      block_id_t bid = inode_p->blocks[i * 2];
      u64 mv_id = inode_p->blocks[i * 2 + 1];
      mac_id_t mac_id = mv_id >> 32;
      if(bid == block_id && mac_id == machine_id){
        inode_p->blocks[i * 2] = KInvalidBlockID;
        inode_p->blocks[i * 2 + 1] = KInvalidBlockID;
        find = true;
      }
    } else {
      inode_p->blocks[i * 2 - 2] = inode_p->blocks[i * 2];
      inode_p->blocks[i * 2 - 1] = inode_p->blocks[i * 2 + 1];
    }
  }

  if(!find)
    return false;

  inode_p->inner_attr.size -= block_size;
  inode_p->inner_attr.set_all_time(time(0));

  auto write_res = bm->write_block(inode_id, inode.data());
  if (write_res.is_err()) {
    return false;
  }

  auto free_res = clients_[machine_id]->call("free_block", block_id);
  fo_mtx.unlock();
  if(free_res.is_err())
    return false;
  auto resp = free_res.unwrap();
  auto res = resp->as<bool>();

  return res;
}


auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  std::list<DirectoryEntry> list;
  auto res = read_directory(this->operation_.get(), node, list);
  if (res.is_err()) {
    return {};
  }

  std::vector<std::pair<std::string, inode_id_t>> ret;
  for (auto &entry : list) {
    ret.push_back({entry.name, entry.id});
  }
  return ret;
}


auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  auto res = this->operation_->get_type_attr(id);
  if (res.is_err()) {
    return {0, 0, 0, 0, 0};
  }

  std::pair<InodeType, FileAttr> attr = res.unwrap();
  return {attr.second.size, attr.second.atime, attr.second.mtime,
          attr.second.ctime, static_cast<u8>(attr.first)};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs