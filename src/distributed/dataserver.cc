#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 1, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    bm->zero_block(0);
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 1, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  auto bm = this->block_allocator_->bm;
  std::vector<u8> version_map(bm->block_size());
  bm->read_block(0, version_map.data());

  if (version_map[block_id] != version) {
    return std::vector<u8>();
  }
  
  std::vector<u8> buffer(bm->block_size());
  bm->read_block(block_id, buffer.data());

  std::vector<u8> res(len);
  memcpy(res.data(), buffer.data() + offset, len);

  return res;
}

auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  auto write_res = this->block_allocator_->bm->write_partial_block(
      block_id, buffer.data(), offset, buffer.size());
  if (write_res.is_err()) {
    return false;
  }

  return true;
}


auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto bm = this->block_allocator_->bm;
  std::vector<u8> version_map(bm->block_size());
  bm->read_block(0, version_map.data());

  auto block_id_res = this->block_allocator_->allocate();
  if (block_id_res.is_err()) {
    return {0, 0};
  }
  
  auto block_id = block_id_res.unwrap();
  version_map[block_id]++;
  bm->write_block(0, version_map.data());

  return {block_id, version_map[block_id]};
}


auto DataServer::free_block(block_id_t block_id) -> bool {
  auto bm = this->block_allocator_->bm;
  std::vector<u8> version_map(bm->block_size());
  bm->read_block(0, version_map.data());

  auto deallocate_res = this->block_allocator_->deallocate(block_id);
  if (deallocate_res.is_err()) {
    return false;
  }

  version_map[block_id]++;
  bm->write_block(0, version_map.data());
  return true;
}
} // namespace chfs