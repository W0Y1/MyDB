#pragma once
#include"TSMFile.h"

class Compact {
public:
	Compact();

	Compact(string path, int id);

	~Compact();

	void SetPath(string path);

	void SetId(int id) { id_ = id; }

	bool Write(map<Key, Values>& data);

	//根据时间戳返回value和value数量count
	bool SearchDataByTime(Key& key, uint64_t timestamp, vector<string>& values);

	//根据value返回时间戳和时间戳数量count
	bool SearchData(Key& key, vector<uint64_t>& times, vector<string>& values);
private:
	string path_;
	int id_;
	TSMFile* tsm_;
};
