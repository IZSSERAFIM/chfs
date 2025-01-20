#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
        auto res = chfs_client->lookup(1, outPutFile);
        if(res.is_err()){
            chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
            return;
        }
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> map;
        map.clear();
        for(auto file : files){
            auto look_res = chfs_client->lookup(1, file);
            if(look_res.is_err()){
                return;
            }
            auto file_inode = look_res.unwrap();
            auto res_type = chfs_client->get_type_attr(file_inode);
            auto len = res_type.unwrap().second.size;
            auto read_res = chfs_client->read_file(file_inode, 0, len);
            auto read_content = read_res.unwrap();
            std::string content(read_content.begin(), read_content.end());
            auto map_content = Map(content);
            map.insert(map.end(), map_content.begin(), map_content.end());
        }
        int offset = 0;
        auto inode = chfs_client->lookup(1, outPutFile).unwrap();
        while(!map.empty()){
            auto key = map[0].key;
            std::vector<std::string> key_buffer;
            key_buffer.clear();
            for(auto it = map.begin();it != map.end();++it){
                if((*it).key == key){
                    key_buffer.push_back((*it).val);
                    map.erase(it);
                    --it;
                }
            }
            auto res = Reduce(key, key_buffer);
            auto write_content = key + " " + res + " ";
            auto buffer = std::vector<chfs::u8>(write_content.begin(), write_content.end());
            auto write_res = chfs_client->write_file(inode, offset, buffer);
            if(write_res.is_ok()){
                offset += buffer.size();
            }
        }
    }
}