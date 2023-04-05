#include "Ring.h"
#include<iostream>
#include<algorithm>
#include<functional>

int HASH_LEN = 32;

uint32_t my_getMurMurHash(const void* key, int len) {
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    const int seed = 97;
    uint32_t h = seed ^ len;
    // Mix 4 bytes at a time into the hash
    const unsigned char* data = (const unsigned char*)key;
    while (len >= 4) {
        uint32_t k = *(uint32_t*)data;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len -= 4;
    }
    // Handle the last few bytes of the input array
    switch (len) {
    case 3:
        h ^= data[2] << 16;
    case 2:
        h ^= data[1] << 8;
    case 1:
        h ^= data[0];
        h *= m;
    };
    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

virtual_node::virtual_node(string key_id, uint32_t hash_value) {
    this->key_id = key_id;
    this->hash_value = hash_value;
}

virtual_node::~virtual_node() {
    data.clear();
}

virtual_node::virtual_node() {
    this->key_id = "";
    this->hash_value = 0;
}

void virtual_node::clear() {
    this->storer_.EraseAll();
}

real_node::real_node(string key_id) {
    this->key_id = key_id;
    this->cur_max_port = 0;
    this->virtual_node_num = 0;
}

real_node::~real_node() {
    this->virtual_node_hash_list.clear();
}

real_node::real_node() {
    this->key_id = "";
    this->cur_max_port = 0;
    this->virtual_node_num = 0;
}

Ring::Ring() {
    this->real_node_sum = 0;
    this->virtual_node_sum = 0;
    add_real_node("RealNode", VIRTUAL_NODE_NUM);
}

Ring::~Ring() {
    this->virtual_node_map.clear();
    this->real_node_map.clear();
    this->sorted_node_hash_list.clear();
}

uint32_t Ring::find_nearest_node(uint32_t hash_value) {
    int low = 0;
    int high = this->sorted_node_hash_list.size() - 1;
    int mid;
    if (hash_value > this->sorted_node_hash_list[high]) {
        return 0;
    }
    while (low < high) {
        mid = (low + high) / 2;
        if (this->sorted_node_hash_list[mid] == hash_value) {
            return mid;
        }
        else if (this->sorted_node_hash_list[mid] > hash_value) {
            high = mid;
        }
        else {//this->sorted_node_hash_list[mid]<data_hash
            low = mid + 1;
        }
    }
    return low;
}

void Ring::add_real_node(string key, uint32_t virtual_node_num) {
    real_node* node;
    if (this->real_node_map.find(key) != this->real_node_map.end()) {
        node = &real_node_map[key];
    }
    else {
        real_node new_node = real_node(key);
        node = &new_node;
        this->real_node_sum++;
    }
    int cur_port = node->cur_max_port;
    int vir_node_num = 0;
    string tmp_key;
    uint32_t tmp_hash;
    while (vir_node_num < virtual_node_num) {
        do {
            cur_port++;
            tmp_key = key + ":" + to_string(cur_port);
            tmp_hash = my_getMurMurHash(tmp_key.c_str(), HASH_LEN);
        } while (this->virtual_node_map.find(tmp_hash) != this->virtual_node_map.end());
        vir_node_num++ ;
        this->virtual_node_map[tmp_hash] = virtual_node(tmp_key, tmp_hash);;
        this->sorted_node_hash_list.push_back(tmp_hash);
        sort(this->sorted_node_hash_list.begin(), this->sorted_node_hash_list.end());
        uint32_t id = this->find_nearest_node(tmp_hash);
        node->virtual_node_hash_list.push_back(tmp_hash);
        uint32_t next_id = id + 1;
        if (next_id >= this->sorted_node_hash_list.size()) {
            next_id = 0;
        }
        uint32_t next_hash = this->sorted_node_hash_list[next_id];
        unordered_map<uint32_t,Key> tobe_deleted;
        unordered_map<uint32_t, Key>* tobe_robbed = &(this->virtual_node_map[next_hash].data);
        for (auto data = tobe_robbed->begin(); data != tobe_robbed->end(); data++) {
            if (data->first < tmp_hash) {
                this->virtual_node_map[tmp_hash].data[data->first] = data->second;
                this->virtual_node_map[tmp_hash].storer_.AddNewData(data->second, this->virtual_node_map[next_hash].storer_.GetTargetData(data->second));
                tobe_deleted[data->first] = data->second;
            }
        }
        for (const auto &deleted : tobe_deleted) {
            this->virtual_node_map[next_hash].storer_.EraseData(deleted.second);
            tobe_robbed->erase(deleted.first);
        }
    }
    node->cur_max_port = cur_port;
    node->virtual_node_num += virtual_node_num;
    this->real_node_map[key] = *node;

    this->virtual_node_sum += virtual_node_num;
}

uint32_t Ring::put(Key& key, string field_value, string value_type, uint64_t timestamp) {
    string data_id;
    data_id += key.series_key_.GetName();
    vector<Tag> tags = key.series_key_.GetTags();
    for (int i = 0; i < key.series_key_.GetNum(); i++) {
        data_id += tags[i].tagkey_;
        data_id += tags[i].tagvalue_;
    }
    data_id += key.field_key_;

    uint32_t data_hash = my_getMurMurHash(data_id.c_str(), HASH_LEN);
    uint32_t id = this->find_nearest_node(data_hash);
    uint32_t put_on_virnode_hash = this->sorted_node_hash_list[id];

    this->virtual_node_map[put_on_virnode_hash].data[data_hash] = key;

    this->virtual_node_map[put_on_virnode_hash].storer_.AddNewData(key, field_value, value_type, timestamp);

    return 0;
}

bool Ring::GetAllData(map<Key, Values>& result) {
    for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
        virtual_node_map[sorted_node_hash_list[i]].storer_.GetData(result);
        virtual_node_map[sorted_node_hash_list[i]].clear();
    }
    //for (vector<uint32_t>::iterator iter = sorted_node_hash_list.begin(); iter != sorted_node_hash_list.end(); iter++) {
    //    virtual_node_map[*iter].storer_->GetData(result);
    //    virtual_node_map[*iter].clear();
    //}
    return !result.empty();
}

void Ring::drop_real_node(string key) {
    deque<uint32_t>* virtual_hash_list_p = &this->real_node_map[key].virtual_node_hash_list;
    sort(virtual_hash_list_p->begin(), virtual_hash_list_p->end());
    uint32_t next_id;
    uint32_t next_hash;
    uint32_t cur_id;
    uint32_t cur_hash;
    vector<uint32_t> tobe_delete;
    for (int i = virtual_hash_list_p->size() - 1; i >= 0; i--) {
        cur_hash = (*virtual_hash_list_p)[i];
        tobe_delete.push_back(cur_hash);
        if (this->virtual_node_map[cur_hash].data.size() > 0) {
            cur_id = this->find_nearest_node(cur_hash);
            next_id = cur_id;
            string next_realnode_key;
            do {
                next_id++;
                if (next_id >= this->sorted_node_hash_list.size()) {
                    next_id = 0;
                }
                next_hash = this->sorted_node_hash_list[next_id];
                next_realnode_key = this->virtual_node_map[next_hash].key_id;
            } while (next_realnode_key.find(key) != -1);

            unordered_map<uint32_t, Key>* moveto = &(this->virtual_node_map[next_hash].data);
            for (auto& data : this->virtual_node_map[cur_hash].data) {
                (*moveto)[data.first] = data.second;
            }
            map<Key, Values>data;
            this->virtual_node_map[cur_hash].storer_.GetData(data);
            this->virtual_node_map[next_hash].storer_.AddNewData(data);

        }
    }
    for (auto hash : tobe_delete) {
        this->virtual_node_map.erase(cur_hash);
        this->virtual_node_sum--;
        auto iter = find(this->sorted_node_hash_list.begin(), this->sorted_node_hash_list.end(), hash);
        if (iter != this->sorted_node_hash_list.end()) {
            this->sorted_node_hash_list.erase(iter);
        }
    }
    sort(this->sorted_node_hash_list.begin(), this->sorted_node_hash_list.end());
    this->real_node_map[key].virtual_node_hash_list.clear();
    this->real_node_map.erase(key);
    this->real_node_sum--;
}

bool Ring::GetTargetData(Key& key, uint64_t timestamp, vector<string>& values) {
    int num = virtual_node_sum / 4;
    thread t1(bind(& Ring::SearchByTime, this, key, timestamp, ref(values), num));
    thread t2(bind(& Ring::SearchByTime, this, key, timestamp, ref(values), num * 2));
    thread t3(bind(& Ring::SearchByTime, this, key, timestamp, ref(values), num * 3));
    thread t4(bind(& Ring::SearchByTime, this, key, timestamp, ref(values), num * 4));
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    if (!values.size())return false;
    return true;
}

bool Ring::GetTargetData(Key& key, vector<string>& values, vector<uint64_t>& times) {
    thread t1(bind(&Ring::SearchByValue, this, key, ref(values), ref(times), 0));
    thread t2(bind(&Ring::SearchByValue, this, key, ref(values), ref(times), 1));
    thread t3(bind(&Ring::SearchByValue, this, key, ref(values), ref(times), 2));
    thread t4(bind(&Ring::SearchByValue, this, key, ref(values), ref(times), 3));
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    if (!times.size())return false;
    return true;
}

void Ring::SearchByTime(Key& key, uint64_t timestamp, vector<string>& values, int flag) {
    int num = VIRTUAL_NODE_NUM / 4;
    for (int i = flag * num; i < flag * (num + 1); i++) {
        virtual_node_map[sorted_node_hash_list[i]].storer_.SearchByTime(key, timestamp, values);
    }
}

void Ring::SearchByValue(Key& key, vector<string>& values, vector<uint64_t>& times, int flag) {
    int num = VIRTUAL_NODE_NUM / 4;
    for (int i = flag * num; i < flag * (num + 1); i++) {
        virtual_node_map[sorted_node_hash_list[i]].storer_.SearchByValue(key, values, times);
    }
}
