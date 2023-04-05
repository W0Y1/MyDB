#pragma once
#include<vector>
#include<string>
#include<array>
#include<unordered_map>
#include"config.h"
using namespace std;

struct TagValue{
	uint16_t value_length_;												//char[2]
	string value_;																//不定
	uint16_t series_num_;													//char[2]
	vector<SeriesId> seriesids_;										//一个id char[4]

	TagValue() {
		value_length_ = 0;
		series_num_ = 0;
	}
	void Init(char* data) {
		int offset = 0;
		char tmp_2[3] = {};
		char tmp_4[5] = {};
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		value_length_ = (uint16_t)atof(tmp_2);

		char* tmp = new char[value_length_ + 1];
		memcpy(tmp, data + offset, value_length_);
		offset += value_length_;
		value_ = tmp;
		delete[]tmp;

		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		series_num_ = (uint16_t)atof(tmp_2);

		for (int i = 0; i < series_num_; i++) {
			memcpy(tmp_4, data + offset, 4);
			offset += 4;
			seriesids_.push_back((SeriesId)atof(tmp_4));
		}
	}
};

struct TagValues {
	array<TagValue, 20> tag_values_;
	unordered_map<string, pair<uint32_t, uint32_t>> tag_value_hash_;				//tagvalue->offset|size			char[16]->char[8]|char[8]
};

struct TagKey{
	uint16_t key_length_;												//char[2]
	string key_;																//不定
	uint32_t tagvalue_hash_offset_;								//char[8]
	uint32_t tagvalue_hash_size_;									//char[8]

	TagKey() {
		key_length_ = 0;
		tagvalue_hash_offset_ = 0;
		tagvalue_hash_size_ = 0;
	}
};

//共char[48]
struct TagBlock_BlockTrailer {
	uint32_t tag_value_offset_;											//char[8]
	uint32_t tag_value_size_;												//char[8]
	uint32_t tag_key_offset_;												//char[8]
	uint32_t tag_key_size_;												//char[8]
	uint32_t tagkey_hash_offset_;										//char[8]
	uint32_t tagkey_hash_size_;											//char[8]
};

class TagBlock {
public:
	friend class TSIFile;
	//根据BlockTrailer读入各部分
	TagBlock();
	 
	void InitBlockTrailer(char data[49]);

	uint32_t GetTagKeyHashOffset() { return block_trailer_.tagkey_hash_offset_; }

	uint32_t GetTagKeyHashSize() { return block_trailer_.tagkey_hash_size_; }

	//插入新的tag以及seriesid 返回block_size和block_offset
	int Insert(string tagkey, string tagvalue, SeriesId seriesid,int* block_size,int*block_offset);

	void UpdateOffset(int change_size);


private:
	array<TagValues, 10> tag_values_;

	array<TagKey, 10> tag_keys_;
	unordered_map<string, pair<uint32_t,uint32_t>> tag_key_hash_;								//tagkey->offset|size			char[16]->char[8]|char[8]

	TagBlock_BlockTrailer block_trailer_;
};
