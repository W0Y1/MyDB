#include "Cache.h"
#include"config.h"


Cache::Cache() {
	size_ = 0;
	max_size_ = CACHE_SIZE;
	ring_ = new Ring;
}

Cache::~Cache() {
	delete ring_;
}

bool Cache::FlushToFile(map<Key,Values>& data) {
	bool flag = ring_->GetAllData(data);
	size_ = sizeof(*ring_);
	return flag;
}

bool Cache::AddData(Key& key, string field_value, string value_type, uint64_t timestamp) {
	if (size_ >= max_size_) return false;
	ring_->put(key, field_value, value_type, timestamp);
	size_ = sizeof(*ring_);
	return true;
}

bool Cache::SearchData(Key key, uint64_t timestamp, vector<string>& values) {
	return ring_->GetTargetData(key, timestamp, values);
}

bool Cache::SearchData(Key key, vector<string>& values, vector<uint64_t>& times) {
	return ring_->GetTargetData(key, values, times);
}