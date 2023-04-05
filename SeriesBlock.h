#pragma once
#include "SeriesKey.h"
#include "SeriesIndex.h"
#include "BPlusTree.h"
#include"BloomFilter.h"

struct SeriesBlock_BlockTrailer {
	int index_entry_root_id;					//char[4]
	uint32_t series_index_offset;			//char[8]
	uint32_t series_index_num;				//char[8]
	uint32_t bloom_filter_offset;			//char[8]
	uint32_t bloom_filter_size;				//char[8]
};

//传进SeriesId找到对应的SeriesKey

class SeriesBlock {               
public:
	friend class TSIFile;

	SeriesBlock();

	~SeriesBlock();

	void InitBlockTrailer(char data[37]);

	void InitBloomFilter(vector<int> data);

	void InitIndex(char* data);

	//寻找SeriesKey是否存在
	bool SearchByFilter(SeriesKey serieskey);

	bool SearchByIndex(SeriesKey serieskey);
	
	//添加在内存中新的SeriesKey
	bool AddSeriesKey(SeriesKey serieskey, int* chang_size);

private:
	BPlusTree* b_plus_tree_;

	//数据部分
	
	BloomFilter* bloom_filter_;
	SeriesBlock_BlockTrailer block_trailer_;

	SeriesIndex seriesindex_entrys_[0];
};