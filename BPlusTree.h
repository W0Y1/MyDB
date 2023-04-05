#pragma once
#include "BPlusTreeInternalEntry.h"
#include "BPlusTreeLeafEntry.h"
#include"config.h"
#include<string>

//删除功能并不完善
class BPlusTree {
public:
    explicit BPlusTree(entry_id_t root_entry_id, SeriesIndex*seriesindex,  int entry_num, int leaf_max_size = LEAF_ENTRY_SIZE, int internal_max_size = INTERNAL_ENTRY_SIZE);

    // Returns true if this B+ tree has no keys and values.
    bool IsEmpty() const;

    entry_id_t GetRootId() { return root_entry_id_; }
    uint32_t GetIndexNum() { return series_index_num; }

    // Insert a key-value pair into this B+ tree.
    bool Insert(const SeriesKey& key);

    // Remove a key and its value from this B+ tree.
    void Remove(const SeriesKey& key);

    // return the value associated with a given key
    bool GetValue(const SeriesKey& key, std::vector<SeriesKey>* result);

    // index iterator
    //Series_Index_Iterator begin();
    //Series_Index_Iterator Begin(const SeriesKey key);
    //Series_Index_Iterator end();

    SeriesIndex* FindLeafEntry(const SeriesKey& key, bool leftMost = false);
private:
    void StartNewTree(const SeriesKey& key);

    bool InsertIntoLeaf(const SeriesKey& key);

    void InsertIntoParent(SeriesIndex* old_node, const SeriesKey& key, SeriesIndex* new_node);

    template <typename N>
    N* Split(N* node);

    template <typename N>
    bool CoalesceOrRedistribute(N* node);

    template <typename N>
    bool FindSibling(N* node, N*& sibling);

    template <typename N>
    bool Coalesce(N* neighbor_node, N* node, BPlusTreeInternalEntry* parent,int index);

    template <typename N>
    void Redistribute(N* neighbor_node, N* node, int index);

    bool AdjustRoot(SeriesIndex* node);

    //数据
	entry_id_t root_entry_id_;
	int leaf_max_size_;
	int internal_max_size_;

    SeriesIndex* seriesindex_entrys_;
    uint32_t series_index_num;
};