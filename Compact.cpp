#include"Compact.h"

Compact::Compact() {
	tsm_ = new TSMFile();
}

Compact::Compact(string path, int id) {
	path_ = path;
	id_ = id;
	tsm_ = new TSMFile(path);
}

Compact::~Compact() {
	delete tsm_;
}

void Compact::SetPath(string path) {
	path_ = path;
	tsm_->SetPath(path);
}

bool Compact::Write(map<Key, Values>& data) {
	for (const auto& p : data) {
		for (auto iter = p.second.values_.begin(); iter != p.second.values_.end(); iter++) {
			tsm_->InsertData(p.first, iter->first, iter->second, p.second.value_type_);			
		}
	}
	tsm_->FlushToFile();
	return true;
}

bool Compact::SearchDataByTime(Key& key, uint64_t timestamp, vector<string>& values) {
	return tsm_->SearchData(key, timestamp, values);
}

bool Compact::SearchData(Key& key, vector<uint64_t>& times, vector<string>& values) {
	return tsm_->SearchData(key, times,values);
}