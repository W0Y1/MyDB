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

	//д������ʱ���������в����Ƿ���ڸ�serieskey
	bool WriteData(SeriesKey& serieskey, string field_key, string field_value, string value_type, uint64_t timestamp);

	//����tag��ѯ����
	bool SearchData(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times,vector<string>& group_info);

	//����tag�Լ�ʱ�����ѯ����
	bool SearchDataByTime(Plan* plan, uint64_t timestamps, vector<string>& result, vector<string>& group_info);

	//�ж��Ƿ�dirty����index��engine�е�����ˢ�µ�������
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