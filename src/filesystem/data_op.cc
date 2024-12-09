#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {
  inode_id_t inode_id = static_cast<inode_id_t>(0);
  auto inode_res = ChfsResult<inode_id_t>(inode_id);

  // 1. Allocate a block for the inode.
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  auto block_id_res = this->block_allocator_->allocate();
  if (block_id_res.is_err()) {
    return ChfsResult<inode_id_t>(block_id_res.unwrap_error());
  }
  auto block_id = block_id_res.unwrap();
  // printf("allocate block_id: %lu\n", block_id);
  inode_res = this->inode_manager_->allocate_inode(type, block_id);
  if (inode_res.is_err()) {
    return ChfsResult<inode_id_t>(inode_res.unwrap_error());
  }

  auto id = inode_res.unwrap();
  // printf("allocate id: %lu\n", id);
  std::vector<u8> inode_data(this->block_manager_->block_size());
  this->inode_manager_->read_inode(id, inode_data);
  auto inode_p = reinterpret_cast<Inode *>(inode_data.data());
  for (uint i = 0; i < inode_p->get_nblocks(); ++i) {
    inode_p->blocks[i] = KInvalidBlockID;
  }

  auto write_res = this->block_manager_->write_block(block_id, inode_data.data());
  if (write_res.is_err()) {
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return inode_res;
}

auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
  return this->inode_manager_->get_attr(id);
}

auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  return this->inode_manager_->get_type_attr(id);
}

auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
  return this->inode_manager_->get_type(id);
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
  return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                     u64 offset) -> ChfsResult<u64> {
  auto read_res = this->read_file(id);
  if (read_res.is_err()) {
    return ChfsResult<u64>(read_res.unwrap_error());
  }

  auto content = read_res.unwrap();
  if (offset + sz > content.size()) {
    content.resize(offset + sz);
  }
  memcpy(content.data() + offset, data, sz);

  auto write_res = this->write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<u64>(write_res.unwrap_error());
  }
  return ChfsResult<u64>(sz);
}

void pushBlockIdToU8Vector(block_id_t block_id, std::vector<u8>& bytes) {
  for (int i = sizeof(block_id) - 1; i >= 0; --i) {
    bytes.push_back(static_cast<u8>(block_id >> (i * 8)));
  }
}

void setBlockIdToU8Vector(block_id_t block_id, std::vector<u8>& bytes, usize offset) {
  for (int i = sizeof(block_id) - 1; i >= 0; --i) {
    bytes[offset + i] = static_cast<u8>(block_id >> (i * 8));
  }
}

block_id_t getBlockIdFromU8Vector(const std::vector<u8>& bytes, usize offset) {
  block_id_t block_id = 0;
  for (usize i = 0; i < sizeof(block_id); ++i) {
    block_id = (block_id << 8) | bytes[offset + i];
  }
  return block_id;
}

auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
    -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inlined_blocks_num = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  } else {
    inlined_blocks_num = inode_p->get_direct_block_num();
  }

  if (content.size() > inode_p->max_file_sz_supported()) {
    std::cerr << "file size too large: " << content.size() << " vs. "
              << inode_p->max_file_sz_supported() << std::endl;
    error_code = ErrorType::OUT_OF_RESOURCE;
    goto err_ret;
  }

  // 2. make sure whether we need to allocate more blocks
  original_file_sz = inode_p->get_size();
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(content.size(), block_size);
  // printf("old_block_num: %u, new_block_num: %u\n", old_block_num, new_block_num);

  if (new_block_num > old_block_num) {
    bool first_indirect_block = true;

    // If we need to allocate more blocks.
    for (usize idx = old_block_num; idx < new_block_num; ++idx) {

      // TODO: Implement the case of allocating more blocks.
      // 1. Allocate a block.
      // 2. Fill the allocated block id to the inode.
      //    You should pay attention to the case of indirect block.
      //    You may use function `get_or_insert_indirect_block`
      //    in the case of indirect block.
      auto allocate_res = this->block_allocator_->allocate();
      if (allocate_res.is_err()) {
        error_code = allocate_res.unwrap_error();
        goto err_ret;
      }
      block_id_t block_id = allocate_res.unwrap();
      // printf("allocate block_id: %lu\n", block_id);
      
      if (inode_p->is_direct_block(idx)) {
        inode_p->blocks[idx] = block_id;
      } else {
        if (first_indirect_block) {
          auto get_res = inode_p->get_or_insert_indirect_block(this->block_allocator_);
          if (get_res.is_err()) {
            error_code = get_res.unwrap_error();
            goto err_ret;
          }
          // printf("get indirect bid: %lu", get_res.unwrap());
          first_indirect_block = false;
          if ((!inode_p->is_direct_block(old_block_num - 1)) && old_block_num != 0){
            std::vector<u8> buffer(this->block_manager_->block_size());
            auto write_res = this->block_manager_->read_block(inode_p->get_indirect_block_id(), buffer.data());
            if (write_res.is_err()) {
              error_code = write_res.unwrap_error();
              goto err_ret;
            }
            size_t num_elements = (old_block_num - inlined_blocks_num) * sizeof(block_id_t);
            indirect_block.insert(indirect_block.end(), buffer.begin(), buffer.begin() + num_elements);
          }
        }
        pushBlockIdToU8Vector(block_id, indirect_block);
        // setBlockIdToU8Vector(block_id, indirect_block, (idx - inlined_blocks_num) * sizeof(block_id_t));
      } 

      if (first_indirect_block) {
        inode_p->invalid_indirect_block_id();
      }
    }

  } else {
    // We need to free the extra blocks.
    if (!inode_p->is_direct_block(old_block_num - 1)) {
      auto indirect_block_id = inode_p->get_indirect_block_id();
      auto res = this->block_manager_->read_block(indirect_block_id, indirect_block.data());
      if (res.is_err()) {
        error_code = res.unwrap_error();
        goto err_ret;
      }
    }

    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      if (inode_p->is_direct_block(idx)) {

        // TODO: Free the direct extra block.
        // printf("free block begin ");
        block_id_t block_id = inode_p->blocks[idx];
        auto res = this->block_allocator_->deallocate(block_id);
        if (res.is_err()) {
          error_code = res.unwrap_error();
          goto err_ret;
        }
        // printf("free block id: %lu\n", block_id);
        inode_p->blocks[idx] = KInvalidBlockID;

      } else {

        // TODO: Free the indirect extra block.
        // printf("free block begin ");
        block_id_t block_id = getBlockIdFromU8Vector(indirect_block, (idx - inlined_blocks_num) * sizeof(block_id_t));
        auto res = this->block_allocator_->deallocate(block_id);
        if (res.is_err()) {
          // printf("deallocate error\n");
          error_code = res.unwrap_error();
          goto err_ret;
        }
        // printf("free block_id: %lu\n", block_id);
        setBlockIdToU8Vector(KInvalidBlockID, indirect_block, (idx - inlined_blocks_num) * sizeof(block_id_t));

      }
    }

    // If there are no more indirect blocks.
    if (old_block_num > inlined_blocks_num &&
        new_block_num <= inlined_blocks_num && true) {

      auto res = this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
      if (res.is_err()) {
        error_code = res.unwrap_error();
        goto err_ret;
      }
      indirect_block.clear();
      inode_p->invalid_indirect_block_id();
    }
  }

  // 3. write the contents
  inode_p->inner_attr.size = content.size();
  inode_p->inner_attr.mtime = time(0);

  {
    auto block_idx = 0;
    u64 write_sz = 0;

    while (write_sz < content.size()) {
      auto sz = ((content.size() - write_sz) > block_size)
                    ? block_size
                    : (content.size() - write_sz);
      std::vector<u8> buffer(block_size);
      // printf("block_idx: %d\n", block_idx);
      memcpy(buffer.data(), content.data() + write_sz, sz);
      // printf("write_sz: %lu, sz: %lu, content.size: %lu, ", write_sz, sz, content.size());

      block_id_t block_id;
      if (inode_p->is_direct_block(block_idx)) {

        // TODO: Implement getting block id of current direct block.
        block_id = inode_p->blocks[block_idx];

      } else {

        // TODO: Implement getting block id of current indirect block.
        block_id = getBlockIdFromU8Vector(indirect_block, (block_idx - inlined_blocks_num) * sizeof(block_id_t));

      }
      // TODO: Write to current block.
      auto write_res = this->block_manager_->write_block(block_id, buffer.data());
      // auto write_res = this->block_manager_->write_partial_block(block_id, buffer.data(), 0, sz);
      
      // printf("write block_id: %lu\n", block_id);
      
      if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
      // printf("write success\n");

      write_sz += sz;
      block_idx += 1;
    }
  }

  // finally, update the inode
  {
    inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {
      error_code = write_res.unwrap_error();
      goto err_ret;
    }
    if (indirect_block.size() != 0) {
      write_res =
          inode_p->write_indirect_block(this->block_manager_, indirect_block);
      if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
    }
  }

  return KNullOk;

err_ret:
  // std::cerr << "write file return error: " << (int)error_code << std::endl;
  return ChfsNullResult(error_code);
}

auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
  auto error_code = ErrorType::DONE;
  std::vector<u8> content;

  const auto block_size = this->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  u64 file_sz = 0;
  u64 read_sz = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  }

  file_sz = inode_p->get_size();
  content.reserve(file_sz);

  // Now read the file
  while (read_sz < file_sz) {
    auto sz = ((inode_p->get_size() - read_sz) > block_size)
                  ? block_size
                  : (inode_p->get_size() - read_sz);
    std::vector<u8> buffer(block_size);

    // Get current block id.
    block_id_t block_id = KInvalidBlockID;
    if (inode_p->is_direct_block(read_sz / block_size)) {
      // TODO: Implement the case of direct block.
      block_id = inode_p->blocks[read_sz / block_size];
    } else {
      // TODO: Implement the case of indirect block.
      auto indirect_block_id = inode_p->get_indirect_block_id();
      auto res = this->block_manager_->read_block(indirect_block_id, indirect_block.data());
      if (res.is_err()) {
        error_code = res.unwrap_error();
        goto err_ret;
      }

      block_id = getBlockIdFromU8Vector(indirect_block, (read_sz / block_size - inode_p->get_direct_block_num()) * sizeof(block_id_t));
    }

    // printf("read_sz: %lu, inode_p->get_size(): %lu\n", read_sz, inode_p->get_size());
    // printf("read block_id: %lu\n", block_id);

    this->block_manager_->read_block(block_id, buffer.data());
    // printf("buffer: %s\n", buffer.data());
    // TODO: Read from current block and store to `content`.
    for(int i = 0; i < sz; i++)
      content.push_back(buffer[i]);

    read_sz += sz;
  }

  // printf("read finish, content: %s\n", content.data());
  // printf("read finish\n");

  return ChfsResult<std::vector<u8>>(std::move(content));

err_ret:
  return ChfsResult<std::vector<u8>>(error_code);
}

auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
  auto res = read_file(id);
  if (res.is_err()) {
    return res;
  }

  auto content = res.unwrap();
  return ChfsResult<std::vector<u8>>(
      std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
}

auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
  auto attr_res = this->getattr(id);
  if (attr_res.is_err()) {
    return ChfsResult<FileAttr>(attr_res.unwrap_error());
  }

  auto attr = attr_res.unwrap();
  auto file_content = this->read_file(id);
  if (file_content.is_err()) {
    return ChfsResult<FileAttr>(file_content.unwrap_error());
  }

  auto content = file_content.unwrap();

  if (content.size() != sz) {
    content.resize(sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err()) {
      return ChfsResult<FileAttr>(write_res.unwrap_error());
    }
  }

  attr.size = sz;
  return ChfsResult<FileAttr>(attr);
}

} // namespace chfs
