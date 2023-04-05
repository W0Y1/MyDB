#pragma once
#include"config.h"
#include<string>
#include<vector>
using namespace std;

class SeriesDataBlock {
public:
	friend class TSMFile;

	SeriesDataBlock();

	//新的DataBlock
	SeriesDataBlock(string type);

	void init(char* data, int size);

	//插入新数据
	//length<500则可继续插入
	bool InsertNewData(uint64_t timestamp, string value);

	//利用二分查找根据时间戳找数据
	bool SearchDataByTime(uint64_t timestamps, string* value);

	//根据数据找时间戳以及数量
	bool GetData(vector<uint64_t>& timestamp, vector<string>& value);

	uint64_t GetMinTime() { return timestamps[0]; }

	uint64_t GetMaxTime() { return timestamps[length_ - 1]; }

	char* GetType() { return type_; }

private:
	//二分查找根据时间戳返回pos
	int BinarySearch(uint64_t timestamp);

	char type_[8];																	//数据类型
	int length_;																		//len(timestamp)	char[3]
	vector<uint64_t>timestamps;											//char[16]
	vector<string>values;														//不定 int:char[12]   double:char[16] bool:char [1] string:char[32]
};
