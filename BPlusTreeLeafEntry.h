#pragma once
#include<iostream>
#include "SeriesIndex.h"
#include "SeriesKey.h"
#include"BPlusTreeInternalEntry.h"

#define LEAF_ENTRY_HEADER_SIZE 24 
#define LEAF_ENTRY_SIZE ((ENTRY_SIZE - LEAF_ENTRY_HEADER_SIZE) / sizeof(pair<SeriesKey, uint32_t>))

// | HEADER | KEY(1) + OFFSET(1) | KEY(2) + OFFSET(2) | ... | KEY(n) + OFFSET(n)
//数据存在4k的尾部
class BPlusTreeLeafEntry :public SeriesIndex {
public:
	~BPlusTreeLeafEntry();
	void Init(entry_id_t Entry_id, entry_id_t parent_id = INVALID_ENTRY_ID, int max_size = LEAF_ENTRY_SIZE);
	void InitInsert(SeriesKey& key, int index) { serieskeys[index] = key; }

	// helper methods
	entry_id_t GetNextEntryId() const;
	void SetNextEntryId(entry_id_t next_Entry_id);
	SeriesKey KeyAt(int index) const;
	int KeyIndex(const SeriesKey& key) const;
	const SeriesKey& GetItem(int index);

	// insert and delete methods
	int Insert(const SeriesKey& key);
	bool Lookup(const SeriesKey& key) const;
	int RemoveAndDeleteRecord(const SeriesKey& key);

	// Split and Merge utility methods
	void MoveHalfTo(BPlusTreeLeafEntry* recipient);
	void MoveAllTo(BPlusTreeLeafEntry* recipient);
	void MoveFirstToEndOf(BPlusTreeLeafEntry* recipient, SeriesIndex* seriesindex_entrys);
	void MoveLastToFrontOf(BPlusTreeLeafEntry* recipient, SeriesIndex* seriesindex_entrys);

private:
	void CopyNFrom(SeriesKey* items, int size);
	void CopyLastFrom(const SeriesKey& item);
	void CopyFirstFrom(const SeriesKey& item, SeriesIndex* seriesindex_entrys);

	entry_id_t next_entry_id_;										//char[3]
	SeriesKey* serieskeys;											//SeriesKey char[256]
};
