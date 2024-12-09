#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}


auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto master_res = metadata_server_->call("mknode", (u8)type, parent, name);
  if(master_res.is_err())
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  auto resp = master_res.unwrap();
  auto res = resp->as<inode_id_t>();
  if(!res)
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  return ChfsResult<inode_id_t>(res);
}


auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto master_res = metadata_server_->call("unlink", parent, name);
  if(master_res.is_err())
    return ChfsNullResult(ErrorType::BadResponse);
  auto resp = master_res.unwrap();
  auto res = resp->as<bool>();
  if(!res)
    return ChfsNullResult(ErrorType::BadResponse);
  return KNullOk;
}


auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto master_res = metadata_server_->call("lookup", parent, name);
  if(master_res.is_err())
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  auto resp = master_res.unwrap();
  auto res = resp->as<inode_id_t>();
  return ChfsResult<inode_id_t>(res);
}


auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto master_res = metadata_server_->call("readdir", id);
  if(master_res.is_err())
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>({});
  auto resp = master_res.unwrap();
  auto res = resp->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res);
}


auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto master_res = metadata_server_->call("get_type_attr", id);
  if(master_res.is_err())
    return ChfsResult<std::pair<InodeType, FileAttr>>({});
  auto resp = master_res.unwrap();
  auto res = resp->as<std::tuple<u64, u64, u64, u64, u8>>();
  InodeType type = static_cast<InodeType>(std::get<4>(res));
  FileAttr attr;
  attr.size = std::get<0>(res);
  attr.atime = std::get<1>(res);
  attr.mtime = std::get<2>(res);
  attr.ctime = std::get<3>(res);
  return ChfsResult<std::pair<InodeType, FileAttr>>({type, attr});
}

/**
 * Read and Write operations are more complicated.
 */

auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  auto get_map_res = metadata_server_->call("get_block_map", id);
  if(get_map_res.is_err())
    return ChfsResult<std::vector<u8>>({});
  auto get_map_resp = get_map_res.unwrap();
  auto block_map = get_map_resp->as<std::vector<BlockInfo>>();
  auto num_block = block_map.size();
  auto read_sz = 0;
  auto cur_block = offset / DiskBlockSize;
  auto cur_offset = offset % DiskBlockSize;
  auto ret = std::vector<u8>();

  while(cur_block < num_block && read_sz < size){
    BlockInfo cur_info = block_map[cur_block];
    block_id_t block_id = std::get<0>(cur_info);
    mac_id_t mac_id = std::get<1>(cur_info);
    version_t version_id = std::get<2>(cur_info);

    auto len = std::min(DiskBlockSize - cur_offset, size - read_sz);
    auto read_res = data_servers_[mac_id]->call("read_data", block_id, cur_offset, len, version_id);
    if(read_res.is_err()){
      return ChfsResult<std::vector<u8>>({});
    }
    auto resp = read_res.unwrap();
    auto res = resp->as<std::vector<u8>>();
    if(res.size() != len){
      return ChfsResult<std::vector<u8>>({});
    }

    ret.insert(ret.end(), res.begin(), res.end());
    cur_block++;
    cur_offset = 0;
    read_sz += len; 
  }

  return ChfsResult<std::vector<u8>>(ret);
}


auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  auto get_map_res = metadata_server_->call("get_block_map", id);
  // printf("get_map_success\n");
  if(get_map_res.is_err())
    return ChfsNullResult(ErrorType::BadResponse);
  auto get_map_resp = get_map_res.unwrap();
  auto block_map = get_map_resp->as<std::vector<BlockInfo>>();
  auto num_block = block_map.size();
  // printf("num_block: %lu\n", num_block);
  auto write_sz = 0;
  auto cur_block = offset / DiskBlockSize;
  auto cur_offset = offset % DiskBlockSize;
  usize size = data.size();

  while(write_sz < size){
    BlockInfo cur_info;
    if(cur_block < num_block){
      cur_info = block_map[cur_block];
    } else {
      auto alloc_res = metadata_server_->call("alloc_block", id);

      if(alloc_res.is_err())
        return ChfsNullResult(ErrorType::BadResponse);
      auto alloc_resp = alloc_res.unwrap();
      cur_info = alloc_resp->as<BlockInfo>();
    }

    block_id_t block_id = std::get<0>(cur_info);
    mac_id_t mac_id = std::get<1>(cur_info);

    auto len = std::min(DiskBlockSize - cur_offset, size - write_sz);
    auto cur_data = std::vector<u8>(data.begin() + write_sz, data.begin() + write_sz + len);
    // printf("write_data: %lu\n", cur_data.size());
    auto write_res = data_servers_[mac_id]->call("write_data", block_id, cur_offset, cur_data);
    // printf("write_data_success\n");
    if(write_res.is_err())
      return ChfsNullResult(ErrorType::BadResponse);
    auto resp = write_res.unwrap();
    auto res = resp->as<bool>();
    if(!res)
      return ChfsNullResult(ErrorType::BadResponse);

    cur_block++;
    cur_offset = 0;
    write_sz += len;
  }

  return KNullOk;
}


auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto master_res = metadata_server_->call("free_block", id, block_id, mac_id);
  if(master_res.is_err())
    return ChfsNullResult(ErrorType::BadResponse);
  auto resp = master_res.unwrap();
  auto res = resp->as<bool>();
  if(!res)
    return ChfsNullResult(ErrorType::BadResponse);
  return KNullOk;
}

} // namespace chfs