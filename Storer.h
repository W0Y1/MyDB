#pragma once
#include<list>
#include<map>
#include"SeriesKey.h"
#include<mutex>

struct Key {
	SeriesKey series_key_;
	char field_key_[16];

	Key() {
		memset(field_key_, 0, 16);
	}
	Key(SeriesKey& serieskey, string field_key) {
		series_key_ = serieskey;
		memset(field_key_, 0, 16);
		memcpy(field_key_, field_key.c_str(),field_key.length());
	}
	bool operator<(const Key& key) const{
		if (series_key_ != key.series_key_) {
			return series_key_ < key.series_key_;
		}
		return field_key_ < key.field_key_;
	}

	bool operator()(const Key& left,const Key& right) const{
		if (left.series_key_ == right.series_key_) {
			return left.field_key_ == right.field_key_;
		}
		return left.series_key_ == right.series_key_;
	}
};

struct Values {
	list<pair<string,uint64_t>> values_;														//field_value|timestamp
	char value_type_[8];
	Values() {
		memset(value_type_, 0, 8);
	}
};

class Storer {
public:
	Storer();

	void operator=(const Storer& storer) {
		data_ = storer.data_;
		key_num_ = storer.key_num_;
	}
	//将所有数据插入到指定map中
	void GetData(map<Key, Values>& data);

	//根据所给Key返回Values
	Values GetTargetData(Key& key);

	uint32_t GetNum() { return key_num_; }

	//将Key对应的Values清除
	bool EraseData(const Key& key);

	void EraseAll();

	void AddNewData(const Key& key,Values values);

	//批量导入
	void AddNewData(const map<Key, Values>& data);

	void AddNewData(Key& key,string field_value,string value_type,uint64_t timestamp);

	//通过时间戳在一个storer中查找数据，数据直接push_back到values中
	void SearchByTime(Key& key, uint64_t timestamp, vector<string>& values);

	// 将对应key的数据直接push_back到values中
	void SearchByValue(Key& key, vector<string>& values, vector<uint64_t>& times);
private:
	map<Key, Values> data_;														//key->field_values|timestamp
	uint32_t key_num_;
	mutex mtx_;
};