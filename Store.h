#pragma once
#include<string>
#include<unordered_map>
#include<time.h>
#include<direct.h>
#include"Plan.h"
#include"Meta.h"
#include "Shard.h"
#include "SeriesFile.h"

using namespace std;

class Store {
public:
	Store();

	~Store();

	bool Init();

	bool SetCurDatabase(string database);

	string GetCurDatabase() { return cur_database_; }

	void ShowDatabases(vector<string>& databases);

	bool CreateNewDatabase(string database);

	vector<ShardInfo> ShowAllShardInfo() { return data_->GetAllShardInfo(); }

	//根据当前数据库和时间戳找到(或创建)shard,并在shard中进行插入
	bool InsertData(SeriesKey& serieskey, string field_key, string field_value, string value_type, const long long timestamp);

	//根据当前数据库找到所有shard，在shard中查询数据
	bool SearchData(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times, vector<string>& group_info);

	//根据提供的时间戳和当前数据库在目标shard中查询数据
	bool SearchDataByTime(Plan* plan, const long long timestamps, vector<string>& result, vector<string>& group_info);

	//将所有需要刷新到磁盘的数据刷新
	void FlushToDisk();
private:
	//被SearchData调用。创建线程在ids[low,high)区间查找shard中的数据
	void CreateThreadToSearch(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times,  vector<string>& group_info,
		vector<int> ids, int low, int high, int* flag);

	void CreateThreadToFlush(int id) { shards_[id]->FlushToDisk(); }

	string cur_database_;
	Meta* data_;

	string path_;
	unordered_map<string, bool> database_status;															//databaes_name->IF_EXIST
	unordered_map<int, Shard*> shards_;																			//shardID->Shard
	unordered_map<string, SeriesFile*> series_files_;															//database_name->SeriesFile									
};
