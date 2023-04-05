#pragma once

#include<string>
#include<vector>
#include<thread>
#include<deque>
#include"Storer.h"
using namespace std;

//virtual_nodeʵ��Ϊreal_node�ķ���
class virtual_node {
public:
    string key_id;                                                                          //��ʶ����real_node
    uint32_t hash_value;
    unordered_map<uint32_t, Key> data;                                                 //keyhash,key

    Storer storer_;                                                                       //����ʵ�ʴ洢����

    virtual_node();

    virtual_node(string key_id, uint32_t hash_value);

    void clear();

    ~virtual_node();

};

//�洢���ڸýڵ��virtual_node
class real_node {
public:
    string key_id;
    uint32_t virtual_node_num;
    deque<uint32_t> virtual_node_hash_list;
    uint32_t cur_max_port;

    real_node();

    real_node(string key_id);

    ~real_node();
};


class Ring {
public:
    uint32_t real_node_sum;
    uint32_t virtual_node_sum;
    unordered_map<string, real_node> real_node_map;
    unordered_map<uint32_t, virtual_node> virtual_node_map;
    vector<uint32_t> sorted_node_hash_list;

    Ring();

    ~Ring();

    uint32_t find_nearest_node(uint32_t hash_value);

    //��������
    uint32_t put(Key& key, string field_value, string value_type, uint64_t timestamp);

    //���ز��������
    bool GetAllData(map<Key, Values>& result);

    bool GetTargetData(Key& key, uint64_t timestamp, vector<string>& values);

    bool GetTargetData(Key& key, vector<string>& values, vector<uint64_t>& times);

    void SearchByTime(Key& key, uint64_t timestamp, vector<string>& values, int index);

    void SearchByValue(Key& key, vector<string>& values, vector<uint64_t>& times, int index);

    //��Ӵ洢�ڵ�
    void add_real_node(string key, uint32_t virtual_node_num);

    void drop_real_node(string key);

};
