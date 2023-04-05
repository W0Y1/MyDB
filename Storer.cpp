#include "Storer.h"
#include<iostream>

Storer::Storer() {
	key_num_ = 0;
}

bool Compare(const pair<string, uint64_t>& p1, const pair<string, uint64_t>& p2) {
	if (p1.first == p2.first) {
		return p1.second < p2.second;
	}
	return p1.first < p2.first;
}


void Storer::GetData(map<Key, Values>& data) {
	if (data_.empty())return;
	data.insert(data_.begin(), data_.end());
}

Values Storer::GetTargetData(Key& key) {
	return data_[key];
}

bool Storer::EraseData(const Key& key) {
	lock_guard<mutex> locker(mtx_);
	if (data_.count(key) == 0) return false;

	data_.erase(key);
	key_num_--;
	return true;
}

void Storer::EraseAll() {
	mtx_.lock();
	data_.clear();
	key_num_ = 0;
	mtx_.unlock();
}

void Storer::AddNewData(const Key& key, Values values) {
	lock_guard<mutex> locker(mtx_);
	if (data_.count(key)) {
		Values* tar_values = &data_[key];
		for (auto iter = values.values_.begin(); iter != values.values_.end(); iter++) {
			tar_values->values_.push_back(*iter);
		}
		tar_values->values_.sort(Compare);
		return ;
	}
	data_[key] = values;
	data_[key].values_.sort(Compare);

}

void Storer::AddNewData(const map<Key, Values>& data) {
	lock_guard<mutex> locker(mtx_);
	if (!data.size()) return;
	for (const auto& p : data) {
		this->AddNewData(p.first, p.second);
	}
}

void Storer::AddNewData(Key& key, string field_value, string value_type, uint64_t timestamp) {
	if (data_.count(key)==0) {
		Values tmp_value;
		tmp_value.values_.push_back({ field_value,timestamp });
		memcpy(tmp_value.value_type_, value_type.c_str(), 8);
		data_[key] = tmp_value;
		return ;
	}
	data_[key].values_.push_back(make_pair(field_value, timestamp));
	data_[key].values_.sort(Compare);
}

void Storer::SearchByTime(Key& key, uint64_t timestamp, vector<string>& values) {
	lock_guard<mutex> locker(mtx_);
	if (data_.count(key) == 0)return;
	Values value = data_[key];
	for (auto iter = value.values_.begin(); iter != value.values_.end(); iter++) {
		if (iter->second == timestamp) {
			values.push_back(iter->first);
			return;
		}
	}
}

void Storer::SearchByValue(Key& key, vector<string>& values, vector<uint64_t>& times) {
	lock_guard<mutex> locker(mtx_);
	if (data_.count(key) == 0)return;
	Values value = data_[key];
	for (auto iter = value.values_.begin(); iter != value.values_.end(); iter++) {
		values.push_back(iter->first);
		times.push_back(iter->second);
	}
	
}