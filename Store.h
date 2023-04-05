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

	//���ݵ�ǰ���ݿ��ʱ����ҵ�(�򴴽�)shard,����shard�н��в���
	bool InsertData(SeriesKey& serieskey, string field_key, string field_value, string value_type, const long long timestamp);

	//���ݵ�ǰ���ݿ��ҵ�����shard����shard�в�ѯ����
	bool SearchData(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times, vector<string>& group_info);

	//�����ṩ��ʱ����͵�ǰ���ݿ���Ŀ��shard�в�ѯ����
	bool SearchDataByTime(Plan* plan, const long long timestamps, vector<string>& result, vector<string>& group_info);

	//��������Ҫˢ�µ����̵�����ˢ��
	void FlushToDisk();
private:
	//��SearchData���á������߳���ids[low,high)�������shard�е�����
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
