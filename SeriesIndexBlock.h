#pragma once
#include"config.h"
#include<string>
#include"Storer.h"

//总大小48
 struct IndexEntry {
	uint64_t min_time_;								//char[16]
	uint64_t max_time_;								//char[16]
	uint32_t offset_;									//char[8]
	uint32_t size_;										//block的大小，根据offset+size可以快速读出		char[8]
};

 struct SeriesIndexFileTrailer {
	 Key key_;
	 char type_[8];										//对应datablock中data的数据类型
	 uint16_t count_;								//index_entrys_的数量				char[2]

	 SeriesIndexFileTrailer() {
		 count_ = 0;
		 memcpy(type_, "", 0);
	 }
};

 //每个SeriesIndexBlock大小固定 4KB	char[4096]
 //IndexEntry从头部开始写 SeriesIndexFileTrailer从尾部开始写
class SeriesIndexBlock {
public:
	friend class TSMFile;

	SeriesIndexBlock();

	//如果当前Key不存在 新的IndexBlock
	SeriesIndexBlock(const Key& key, const char type[8]);

	//读到TSMFileTrailer后，根据offset每4KB一个SeriesIndexBlock
	void init(char data[4096]);

	//根据时间戳使用max_time和min_time定位，返回offset和size
	bool SearchDataBlock(uint64_t time, vector<pair<int, int>>& data);

	//根据field_value查找时返回所有datablock
	bool SearchDataBlock(vector<pair<int, int>>& data);

	//当新增一个新的datablock后需要新增一个新的indexentry
	void AddNewIndexEntry(uint64_t min_time, uint64_t max_time, uint32_t size, uint32_t offset);

	//当SeriesDataBlock更新完数据后需要更新相应IndexEntry
	void Update(uint64_t min_time, uint64_t max_time, uint32_t change_size,uint32_t pos_entry);

	uint16_t GetCount() { return file_trailer_.count_; }

private:
	vector<IndexEntry> index_entrys_;

	SeriesIndexFileTrailer file_trailer_;
};