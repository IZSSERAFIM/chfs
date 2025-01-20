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

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if(res.is_err()){
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  auto id = res.unwrap()->as<inode_id_t>();
  if(id == KInvalidInodeID){
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  return ChfsResult<inode_id_t>(id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = metadata_server_->call("unlink", parent, name);
  if(res.is_err()){
    return ChfsNullResult(res.unwrap_error());
  }
  auto success = res.unwrap()->as<bool>();
  if(!success){
    return ChfsNullResult(ErrorType::INVALID);
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = metadata_server_->call("lookup", parent, name);
  if(res.is_err()){
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  auto id = res.unwrap()->as<inode_id_t>();
  if(id == KInvalidInodeID){
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  return ChfsResult<inode_id_t>(id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = metadata_server_->call("readdir", id);
  if(res.is_err()){
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res.unwrap_error());
  }
  auto vec = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(vec);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = metadata_server_->call("get_type_attr", id);
  if(res.is_err()){
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
  }
  auto attr_res = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto type = std::get<4>(attr_res);
  InodeType inode_type;
  if(type == DirectoryType){
    inode_type = InodeType::Directory;
  }
  else if(type == RegularFileType){
    inode_type = InodeType::FILE;
  }
  else{
    inode_type = InodeType::Unknown;
  }
  FileAttr file_attr;
  file_attr.size = std::get<0>(attr_res);
  file_attr.atime = std::get<1>(attr_res);
  file_attr.mtime = std::get<2>(attr_res);
  file_attr.ctime = std::get<3>(attr_res);
  return ChfsResult<std::pair<InodeType, FileAttr>>(std::pair<InodeType, FileAttr>(inode_type, file_attr));
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCKSIZE = DiskBlockSize;
  auto res = metadata_server_->call("get_block_map", id);
  if(res.is_err()){
    return ChfsResult<std::vector<u8>>(res.unwrap_error());
  }
  auto block_map = res.unwrap()->as<std::vector<BlockInfo>>();
  auto flie_size = block_map.size() * BLOCKSIZE;
  if(offset + size >= flie_size){
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
  }
  std::vector<u8> buffer(size);
  auto start_index = offset / BLOCKSIZE;
  auto start_offset = offset % BLOCKSIZE;
  auto end_index = ((offset + size) % BLOCKSIZE) ? ((offset + size) / BLOCKSIZE + 1) : ((offset + size) / BLOCKSIZE);
  auto end_offset = ((offset + size) % BLOCKSIZE) ? ((offset + size) % BLOCKSIZE) : BLOCKSIZE;
  usize buffer_offset = 0;
  for(auto i = block_map.begin() + start_index; i != block_map.begin() + end_index; i++){
    block_id_t block_id = std::get<0>(*i);
    mac_id_t mac_id = std::get<1>(*i);
    version_t version = std::get<2>(*i);
    auto it = data_servers_.find(mac_id);
    if(it == data_servers_.end()){
      return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
    }
    auto cli = it->second;
    auto read_res = cli->call("read_data", block_id, 0, BLOCKSIZE, version);
    if(read_res.is_err()){
      return ChfsResult<std::vector<u8>>(read_res.unwrap_error());
    }
    auto block_buffer = read_res.unwrap()->as<std::vector<u8>>();
    if(i == block_map.begin() + start_index && i == block_map.begin() + end_index - 1){
      std::copy(block_buffer.begin() + start_offset, block_buffer.begin() + end_offset, buffer.begin());
      return ChfsResult<std::vector<u8>>(buffer);
    }
    else if(i == block_map.begin() + start_index){
      std::copy(block_buffer.begin() + start_offset, block_buffer.end(), buffer.begin());
      buffer_offset += BLOCKSIZE - start_offset;
    }
    else if(i == block_map.begin() + end_index - 1){
      std::copy_n(block_buffer.begin(), end_offset, buffer.begin() + buffer_offset);
      buffer_offset += end_offset;
    }
    else{
      std::copy_n(block_buffer.begin(), BLOCKSIZE, buffer.begin() + buffer_offset);
      buffer_offset += BLOCKSIZE;
    }
  }
  return ChfsResult<std::vector<u8>>(buffer);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCKSIZE = DiskBlockSize;
  auto len = data.size();
  auto res = metadata_server_->call("get_block_map", id);
  if(res.is_err()){
    return ChfsNullResult(res.unwrap_error());
  }
  auto block_map = res.unwrap()->as<std::vector<BlockInfo>>();
  auto flie_size = block_map.size() * BLOCKSIZE;
  if(offset + len > flie_size){
    auto new_block_num = (offset + len) % BLOCKSIZE ? (offset + len) / BLOCKSIZE + 1 : (offset + len) / BLOCKSIZE;
    auto old_block_num = block_map.size();
    for(auto i = old_block_num; i < new_block_num; i++){
      auto allocate_res = metadata_server_->call("alloc_block", id);
      if(allocate_res.is_err()){
        return ChfsNullResult(allocate_res.unwrap_error());
      }
      auto block_info = allocate_res.unwrap()->as<BlockInfo>();
      block_map.push_back(block_info);
    }
  }
  auto start_index = offset / BLOCKSIZE;
  auto start_offset = offset % BLOCKSIZE;
  auto end_index = ((offset + len) % BLOCKSIZE) ? ((offset + len) / BLOCKSIZE + 1) : ((offset + len) / BLOCKSIZE);
  auto end_offset = ((offset + len) % BLOCKSIZE) ? ((offset + len) % BLOCKSIZE) : BLOCKSIZE;
  usize buffer_offset = 0;
  for(auto i = block_map.begin() + start_index; i != block_map.begin() + end_index; i++){
    block_id_t block_id = std::get<0>(*i);
    mac_id_t mac_id = std::get<1>(*i);
    auto it = data_servers_.find(mac_id);
    if(it == data_servers_.end()){
      return ChfsNullResult(ErrorType::INVALID_ARG);
    }
    auto cli = it->second;
    std::vector<u8> block_buffer;
    usize write_offset = 0;
    if(i == block_map.begin() + start_index && i == block_map.begin() + end_index - 1){
      auto write_res = cli->call("write_data", block_id, start_offset, data);
      if(write_res.is_err()){
        return ChfsNullResult(write_res.unwrap_error());
      }
      auto success = write_res.unwrap()->as<bool>();
      if(!success){
        return ChfsNullResult(ErrorType::INVALID);
      }
      return KNullOk;
    }
    else if(i == block_map.begin() + start_index){
      block_buffer.resize(BLOCKSIZE - start_offset);
      std::copy_n(data.begin(), BLOCKSIZE - start_offset, block_buffer.begin());
      write_offset = start_offset;
      buffer_offset += BLOCKSIZE - start_offset;
    }
    else if(i == block_map.begin() + end_index - 1){
      block_buffer.resize(end_offset);
      std::copy_n(data.begin() + buffer_offset, end_offset, block_buffer.begin());
      write_offset = 0;
      buffer_offset += end_offset;
    }
    else{
      block_buffer.resize(BLOCKSIZE);
      std::copy_n(data.begin() + buffer_offset, BLOCKSIZE, block_buffer.begin());
      write_offset = 0;
      buffer_offset += BLOCKSIZE;
    }
    auto cli_write_res = cli->call("write_data", block_id, write_offset, block_buffer);
    if(cli_write_res.is_err()){
      return ChfsNullResult(cli_write_res.unwrap_error());
    }
    auto success = cli_write_res.unwrap()->as<bool>();
    if(!success){
      return ChfsNullResult(ErrorType::INVALID);
    }
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  if(res.is_err()){
    return ChfsNullResult(res.unwrap_error());
  }
  auto success = res.unwrap()->as<bool>();
  if(!success){
    return ChfsNullResult(ErrorType::NotExist);
  }
  return KNullOk;
}

} // namespace chfs