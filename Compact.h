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

	//����ʱ�������value��value����count
	bool SearchDataByTime(Key& key, uint64_t timestamp, vector<string>& values);

	//����value����ʱ�����ʱ�������count
	bool SearchData(Key& key, vector<uint64_t>& times, vector<string>& values);
private:
	string path_;
	int id_;
	TSMFile* tsm_;
};
