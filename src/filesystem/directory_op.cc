#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // Append the new directory entry to `src`.
  src += filename + ":" + inode_id_to_string(id) + "/";
  
  return src;
}

void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  std::string name;
  std::string inode_id;

  for (auto i = 0; i < src.size(); i++) {
    if (src[i] == '/') {
      list.push_back({name, string_to_inode_id(inode_id)});
      name.clear();
      inode_id.clear();
    } else if (src[i] == ':') {
      name = inode_id;
      inode_id.clear();
    } else {
      inode_id += src[i];
    }
  }

}

auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  //  Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);

  for (auto &entry : list) {
    if (entry.name == filename) {
      continue;
    }
    res = append_to_directory(res, entry.name, entry.id);
  }

  return res;
}

auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  list.clear();
  auto res = fs->read_file(id);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }
  std::vector<u8> data = res.unwrap();
  std::string str(data.begin(), data.end());
  // printf("read_dir: %s\n", str.c_str());
  parse_directory(str, list);

  return KNullOk;
}

auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;
  auto res = read_directory(this, id, list);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  for (const auto &entry : list) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }
  
  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  std::list<DirectoryEntry> list;
  auto res = read_directory(this, id, list);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  for (const auto &entry : list) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  }

  auto alloc_res = this->alloc_inode(type);
  if (alloc_res.is_err()) {
    return ChfsResult<inode_id_t>(alloc_res.unwrap_error());
  }
  inode_id_t new_inode_id = alloc_res.unwrap();

  std::string dir_content = dir_list_to_string(list);
  if (!dir_content.empty()){
    dir_content += "/";
  }
  // printf("dir_content: %s \n", dir_content.c_str());
  dir_content = append_to_directory(dir_content, name, new_inode_id);
  std::vector<u8> data(dir_content.begin(), dir_content.end());

  // std::string str(data.begin(), data.end());
  // printf("data: %s \n", str.c_str());

  auto write_res = this->write_file(id, data);
  if (write_res.is_err()) {
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return ChfsResult<inode_id_t>(new_inode_id);
}

auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  std::list<DirectoryEntry> list;
  list.clear();
  auto read_res = read_directory(this, parent, list);
  if (read_res.is_err()) {
    return ChfsNullResult(read_res.unwrap_error());
  }

  auto lookup_res = this->lookup(parent, name);
  if (lookup_res.is_err()) {
    return ChfsNullResult(lookup_res.unwrap_error());
  }
  inode_id_t inode_id = lookup_res.unwrap();

  auto remove_res = this->remove_file(inode_id);
  if (remove_res.is_err()) {
    return ChfsNullResult(remove_res.unwrap_error());
  }

  std::string dir_content = dir_list_to_string(list);
  if (!dir_content.empty()){
    dir_content += "/";
  }
  dir_content = rm_from_directory(dir_content, name);
  std::vector<u8> data(dir_content.begin(), dir_content.end());

  auto write_res = this->write_file(parent, data);
  if (write_res.is_err()) {
    return ChfsNullResult(write_res.unwrap_error());
  }
  
  return KNullOk;
}

} // namespace chfs
