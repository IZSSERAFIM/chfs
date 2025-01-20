#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        auto look_res = chfs_client->lookup(1, filename);
        auto inode_id = look_res.unwrap();
        auto type_res = chfs_client->get_type_attr(inode_id);
        auto len = type_res.unwrap().second.size;
        auto read_res = chfs_client->read_file(inode_id, 0, len);
        auto read_content = read_res.unwrap();
        std::string content(read_content.begin(), read_content.end());
        auto map = Map(content);
        auto mk_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(index));
        auto inode = mk_res.unwrap();
        std::stringstream str_stream("");
        for(auto &i : map){
            str_stream << i.key << " " << i.val << " ";
        }
        auto str = str_stream.str();
        std::vector<chfs::u8> buffer(str.begin(), str.end());
        chfs_client->write_file(inode, 0, buffer);
        doSubmit(mapReduce::mr_tasktype::MAP, index);
    }

    void Worker::doReduce(int index, int nfiles, int nreduces) {
        // Lab4: Your code goes here.
        int begin = (26 / nreduces) * index;
        int end;
        if(index == nreduces - 1){
            end = 26;
        }
        else{
            end = begin + (26 / nreduces);
        }
        std::vector<KeyVal> targets;
        targets.clear();
        auto mk_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "reduce-" + std::to_string(index));
        auto inode = mk_res.unwrap();
        for(int i = 0;i < nfiles;++i){
            auto look_res = chfs_client->lookup(1, "mr-" + std::to_string(i));
            auto inode_id = look_res.unwrap();
            auto type_res = chfs_client->get_type_attr(inode_id);
            auto len = type_res.unwrap().second.size;
            auto read_res = chfs_client->read_file(inode_id, 0, len);
            auto read_content = read_res.unwrap();
            std::string content(read_content.begin(), read_content.end());
            std::stringstream str_stream(content);
            std::string key, value;
            while(str_stream >> key >> value){
                if(!key.empty() && ((key[0] >= ('a' + begin) && key[0] < ('a' + end)) || (key[0] >= ('A' + begin) && key[0] < ('A' + end)))){
                    targets.push_back(KeyVal(key, value));
                }
            }
        }
        std::string content_to_write = "";
        while(!targets.empty()){
            auto key = targets[0].key;
            std::vector<std::string> key_buffer;
            key_buffer.clear();
            for(auto it = targets.begin();it != targets.end();++it){
                if((*it).key == key){
                    key_buffer.push_back((*it).val);
                    targets.erase(it);
                    --it;
                }
            }
            auto reduce_res = Reduce(key, key_buffer);
            content_to_write.append(key + " " + reduce_res + " ");
        }
        auto write_buffer = std::vector<chfs::u8>(content_to_write.begin(), content_to_write.end());
        auto write_res = chfs_client->write_file(inode, 0, write_buffer);
        if(write_res.is_ok()){
            doSubmit(mapReduce::mr_tasktype::REDUCE, index);
        }
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
    }

    void Worker::doMerge(int nreduces){
        auto look_res = chfs_client->lookup(1, outPutFile);
        auto inode = look_res.unwrap();
        int offset = 0;
        for(int i = 0; i < nreduces; ++i){
            auto res = chfs_client->lookup(1, "reduce-" + std::to_string(i));
            auto inode_id = res.unwrap();
            auto type_res = chfs_client->get_type_attr(inode_id);
            auto len = type_res.unwrap().second.size;
            auto read_res = chfs_client->read_file(inode_id, 0, len);
            auto read_content = read_res.unwrap();
            auto write_res = chfs_client->write_file(inode, offset, read_content);
            if(write_res.is_ok()){
                offset += read_content.size();
            }
        }
        doSubmit(mapReduce::mr_tasktype::NONE, 0);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto call_res = mr_client->call(ASK_TASK, 0);
            if(call_res.is_err()){
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            else{
                auto res = call_res.unwrap()->as<std::tuple<int, int, std::string, int, int>>();
                int type = std::get<0>(res);
                if(type == mapReduce::mr_tasktype::NONE){
                    int nreduces = std::get<3> (res);
                    int nfiles = std::get<4>(res);
                    if(nreduces > 0 && nfiles > 0){
                        doMerge(nreduces);
                    }
                    else{
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                }
                else if(type == mapReduce::mr_tasktype::MAP){
                    int index = std::get<1>(res);
                    std::string filename = std::get<2>(res);
                    doMap(index, filename);
                }
                else{
                    int index = std::get<1>(res);
                    int nreduces = std::get<3> (res);
                    int nfiles = std::get<4>(res);
                    doReduce(index, nfiles, nreduces);
                }
            }
        }
    }
}