#pragma once
#include "SeriesIndex.h"
#include "SeriesKey.h"

#define INTERNAL_ENTRY_HEADER_SIZE 20
#define INTERNAL_ENTRY_SIZE ((ENTRY_SIZE - INTERNAL_ENTRY_HEADER_SIZE) / (sizeof(pair<SeriesKey, entry_id_t>)))

// | HEADER | KEY(1)+ENTRY_ID(1) | KEY(2)+ENTRY_ID(2) | ... | KEY(n)+ENTRY_ID(n) |
//array[0]的SeriesKey不做存储
//数据存在4k的尾部
class BPlusTreeInternalEntry :public SeriesIndex {
public:
	void Init(entry_id_t entry_id, entry_id_t parent_id = INVALID_ENTRY_ID, int max_size = INTERNAL_ENTRY_SIZE);
	void InitInsert(SeriesKey& key, int entry_id, int index) { array[index] = { key,entry_id }; }
	
	SeriesKey KeyAt(int index) const;
	void SetKeyAt(int index, const SeriesKey& key);
	int ValueIndex(const entry_id_t& value) const;
	entry_id_t ValueAt(int index) const;

	entry_id_t Lookup(const SeriesKey& key) const;
	void PopulateNewRoot(const entry_id_t& old_value, const SeriesKey& new_key, const entry_id_t& new_value);
	int InsertNodeAfter(const entry_id_t& old_value, const SeriesKey& new_key, const entry_id_t& new_value);
	void Remove(int index);
	entry_id_t RemoveAndReturnOnlyChild();


	// Split and Merge utility methods
	void MoveAllTo(BPlusTreeInternalEntry* recipient, const int& middle_key,  SeriesIndex* seriesindex_entrys);
	void MoveHalfTo(BPlusTreeInternalEntry* recipient, SeriesIndex* seriesindex_entrys);
	void MoveFirstToEndOf(BPlusTreeInternalEntry* recipient, const int& middle_key, SeriesIndex* seriesindex_entrys);
	void MoveLastToFrontOf(BPlusTreeInternalEntry* recipient, const int& middle_key, SeriesIndex* seriesindex_entrys);

private:
	void CopyNFrom(pair<SeriesKey, entry_id_t>* items, int size);
	void CopyLastFrom(const pair<SeriesKey, entry_id_t>& pair, SeriesIndex* seriesindex_entrys);
	void CopyFirstFrom(const pair<SeriesKey, entry_id_t>& pair, SeriesIndex* seriesindex_entrys);
	pair<SeriesKey, entry_id_t> array[20];			//<char[256],char[3]>
};