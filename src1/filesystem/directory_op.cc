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

// {Your code here}
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

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  // UNIMPLEMENTED();
  std::string str;
  if(!src.empty()){
    str += '/';
  }
  str += filename + ':' + std::to_string(id);
  src += str;
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if(src.empty()){
    return;
  }
  list.clear();
  std::istringstream iss(src);
  std::string token;
  while(std::getline(iss, token, '/')){
    std::istringstream token_stream(token);
    DirectoryEntry tmp;
    std::getline(token_stream, tmp.name, ':');
    token_stream >> tmp.id;
    list.push_back(tmp);
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  // UNIMPLEMENTED();
  if(src.empty()){
    return src;
  }
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for(auto it = list.begin(); it != list.end(); it++){
    if(it->name != filename){
      res = append_to_directory(res, it->name, it->id);
    }
  }
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::vector<u8> res = (fs->read_file(id)).unwrap();
  std::string content;
  content.assign(res.begin(), res.end());
  parse_directory(content, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  // UNIMPLEMENTED();
  read_directory(this, id, list);
  for(auto it = list.begin(); it != list.end(); it++){
    if(it->name == name){
      return ChfsResult<inode_id_t>(it->id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  // UNIMPLEMENTED();
  std::list<DirectoryEntry> list;
  std::string filename(name, strlen(name));
  read_directory(this, id, list);
  for(auto it = list.begin(); it != list.end(); it++){
    if(it->name == filename){
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  }

  inode_id_t allocate_inode_id = (this->alloc_inode(type)).unwrap();
  std::string new_dir_string = dir_list_to_string(list);
  new_dir_string = append_to_directory(new_dir_string, name, allocate_inode_id);
  std::vector<u8> buffer(new_dir_string.begin(), new_dir_string.end());
  this->write_file(id, buffer);

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(allocate_inode_id));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  // UNIMPLEMENTED();
  inode_id_t remove_file_inode_id = (this->lookup(parent, name)).unwrap();
  this->remove_file(remove_file_inode_id);
  std::string filename(name, strlen(name));
  std::vector<u8> parent_content = (this->read_file(parent)).unwrap();
  std::string parent_content_str(reinterpret_cast<char *>(parent_content.data()), parent_content.size());
  std::string parent_content_str_change = rm_from_directory(parent_content_str, filename);
  std::vector<u8> buffer(parent_content_str_change.begin(), parent_content_str_change.end());
  this->write_file(parent, buffer);
  
  return KNullOk;
}

} // namespace chfs
