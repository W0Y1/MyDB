#include"SeriesBlock.h"

SeriesBlock::SeriesBlock() {
	block_trailer_.bloom_filter_offset = 0;
	block_trailer_.bloom_filter_size = 0;
	block_trailer_.series_index_num = 0;
	block_trailer_.series_index_offset = 0;
	block_trailer_.index_entry_root_id = -1;
	b_plus_tree_ = new BPlusTree(block_trailer_.index_entry_root_id, seriesindex_entrys_, block_trailer_.series_index_num);
	bloom_filter_ = new BloomFilter(BLOOM_FILTER_SIZE);
}

SeriesBlock::~SeriesBlock() {
	delete bloom_filter_;
	delete b_plus_tree_;
}

void SeriesBlock::InitBlockTrailer(char data[37]) {
	char tmp_4[5] = {};
	char tmp_8[9] = {};
	int offset = 0;
	memcpy(tmp_4, data + offset, 4);
	offset += 4;
	block_trailer_.index_entry_root_id = atof(tmp_4);
	memcpy(tmp_8, data+offset, 8);
	offset += 8;
	block_trailer_.series_index_offset = atof(tmp_8);
	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.series_index_num = atof(tmp_8);
	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.bloom_filter_offset = atof(tmp_8);
	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.bloom_filter_size = atof(tmp_8);
}

void SeriesBlock::InitBloomFilter(vector<int> data) {
	bloom_filter_->Init(data);
}

void SeriesBlock::InitIndex(char* data) {
	delete b_plus_tree_;
	b_plus_tree_ = new BPlusTree(block_trailer_.index_entry_root_id, seriesindex_entrys_, block_trailer_.series_index_num);
	int offset = 0;
	char tmp_1;
	char tmp_2[3] = {};
	char tmp_3[4] = {};
	char tmp_256[257] = {};
	for (int i = 0; i < block_trailer_.series_index_num; i++) {
		offset = i * SERIIES_INDEX_BLOCK_SIZE;
		memcpy((char*)&tmp_1, data + offset, 1);
		offset += 1;

		if (tmp_1 == '1') {
			SeriesIndex* entry = &seriesindex_entrys_[i];
			auto new_leaf = reinterpret_cast<BPlusTreeLeafEntry*>(entry);
			new_leaf->Init(i);
			
			memcpy(tmp_2, data + offset, 2);
			offset += 2;
			new_leaf->SetSize(atof(tmp_2));

			memcpy(tmp_3, data + offset, 3);
			offset += 3;
			new_leaf->SetParentEntryId(atof(tmp_3));

			memcpy(tmp_3, data + offset, 3);
			offset += 3;
			new_leaf->SetNextEntryId(atof(tmp_3));

			SeriesKey serieskey;
			for (int j = 0; j < new_leaf->GetSize(); j++) {
				memcpy(tmp_256, data + offset, 256);
				offset += 256;

				serieskey.Init(tmp_256);
				new_leaf->InitInsert(serieskey, j);
			}
		}
		else {
			SeriesIndex* entry = &seriesindex_entrys_[i];
			auto new_internal = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
			new_internal->Init(i);

			memcpy(tmp_2, data + offset, 2);
			offset += 2;
			new_internal->SetSize(atof(tmp_2));

			memcpy(tmp_3, data + offset, 3);
			offset += 3;
			new_internal->SetParentEntryId(atof(tmp_3));

			SeriesKey serieskey;
			for (int j = 0; j < new_internal->GetSize(); j++) {
				memcpy(tmp_256, data + offset, 256);
				offset += 256;
				memcpy(tmp_3, data + offset, 3);
				offset += 3;

				serieskey.Init(tmp_256);      
				new_internal->InitInsert(serieskey, atof(tmp_3), j);
			}
		}
	}
	delete b_plus_tree_;
	b_plus_tree_ = new BPlusTree(block_trailer_.index_entry_root_id, seriesindex_entrys_, block_trailer_.series_index_num);
}

bool SeriesBlock::SearchByFilter(SeriesKey serieskey) {
	 bool result = bloom_filter_->Find(serieskey.ToString());
	 delete bloom_filter_;
	 return result;
}

bool SeriesBlock::SearchByIndex(SeriesKey serieskey) {
	SeriesIndex* entry = b_plus_tree_->FindLeafEntry(serieskey, false);
	auto leaf_page = reinterpret_cast<BPlusTreeLeafEntry*>(entry);

	bool result =  leaf_page->Lookup(serieskey);
	delete b_plus_tree_;
	return result;
}

bool SeriesBlock::AddSeriesKey(SeriesKey serieskey, int* change_size) {
	if (!b_plus_tree_->Insert(serieskey)) return false;
	block_trailer_.index_entry_root_id = b_plus_tree_->GetRootId();
	int change_num = b_plus_tree_->GetIndexNum() - block_trailer_.series_index_num;
	*change_size = change_num * ENTRY_SIZE;
	block_trailer_.bloom_filter_offset += *change_size;
	block_trailer_.series_index_num = b_plus_tree_->GetIndexNum();
	bloom_filter_->Set(serieskey.ToString());
	if (block_trailer_.bloom_filter_size != (BLOOM_FILTER_SIZE / 32 + 1) * 12) {
		block_trailer_.bloom_filter_size = (BLOOM_FILTER_SIZE / 32 + 1) * 12;
		*change_size += block_trailer_.bloom_filter_size;
	}
	return true;
}