#pragma once
#include<string>
#include<direct.h>
#include "Engine.h"
#include "Index.h"
#include"Plan.h"
using namespace std;

class Shard {
public:
	Shard();

	Shard(SeriesFile* seriesfile);

	~Shard();

	void SetPath(string path);

	void SetId(int id) { id_ = id; }

	void SetDatabase(string database) { database_ = database; }

	//写入数据时先在索引中查找是否存在该serieskey
	bool WriteData(SeriesKey& serieskey, string field_key, string field_value, string value_type, uint64_t timestamp);

	//根据tag查询数据
	bool SearchData(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times,vector<string>& group_info);

	//根据tag以及时间戳查询数据
	bool SearchDataByTime(Plan* plan, uint64_t timestamps, vector<string>& result, vector<string>& group_info);

	//判断是否dirty并将index和engine中的数据刷新到磁盘中
	void FlushToDisk();
private:
	void CreateThreadToSearch(vector<SeriesKey>keys, string field_key, vector<string>& values, vector<uint64_t>& times);
	void CreateThreadToSearchByTime(vector<SeriesKey>keys, string field_key, uint64_t timestamp, vector<string>& values);

	string path_;
	int id_;
	string database_;
	bool is_dirty;

	Index* index_;
	Engine* engine_;
};