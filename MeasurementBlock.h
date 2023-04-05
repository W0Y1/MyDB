#pragma once
#include<unordered_map>
#include<string>
#include<array>
using namespace std;

struct Measurement {
	string name_;													//char[16]
	uint32_t tag_block_offset_;								//char[8]
	uint32_t tag_block_size_;									//char[8]

	Measurement() {
		tag_block_offset_ = 0;
		tag_block_size_ = 0;
	}
};

struct MeasurementBlockTrailer {
	uint32_t data_offset_;										//char[8]
	uint32_t data_size_;											//char[8]
	uint32_t hash_offset_;										//char[8]
	uint32_t hash_size_;											//char[8]
};

class MeasurementBlock {
public:
	friend class TSIFile;
	//根据BlockTrailer读入各部分
	MeasurementBlock();

	void Init(char* data, int size);

	//插入数据 如果measurement不存在则新建一个 返回 pos_measurements 和tagblock的offset
	//返回flag==0代表是已经存在的measurement 反之
	int Insert(string name, int* tagblock_offset, int* flag);

	//输入参数:name对应measurement 
	//返回两个指针为measurement对应的tagblock的信息
	bool SearchTagBlock(string name,uint32_t* tag_block_offset, uint32_t* tag_block_size);

	//当TagBlock做出改变时需要调用 需要将该Measurement后续的Measurement也更新
	//输入参数:measurement为改变的TagBlock所对应的
	void UpdateMeasurement(int tar_index ,uint32_t tag_block_offset,uint32_t tag_block_size);

	void UpdateBlockTrailer(int change_size);

	//将所有offset信息更新 并返回hash_index的size
	int UpdateOffset(int change_size);

private:
	array<Measurement,100> measurements_;
	unordered_map<string, uint16_t> hash_index_;										//name->pos_Measurement				char[8]->char[2]
	MeasurementBlockTrailer block_trailer_;
};
