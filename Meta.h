#pragma once
#include"Shard.h"
#include"helper.h"
#include<direct.h>

struct ShardInfo {
	bool tomb;																	//char[1]
	int id_;																			//char[4]
	string owner_;																//char[16]
	tm start_time_;																//char[14]
	tm end_time_;																//char[14]
	tm delete_time_;															//char[14]
};

class Meta {
public:
	Meta();

	~Meta();

	//��meta�ļ��ж�ȡ����,����ʱ�䵽�ڵ�shardɾ��
	bool Init();

	//����database��time���ض�Ӧshard��idֵ,���������ɵ�shardˢ�µ�������
	//flagΪ0�Ǳ���ѯ���� Ϊ1�Ǳ��������
	int GetShard(string database, tm time, bool flag);

	//����ָ��database�µ�����shard 
	void GetShard(string database, vector<int>& ids);

	unordered_map<string, vector<ShardInfo>> GetAllShard() {return DatabaseShardMap_;}

	vector<ShardInfo> GetAllShardInfo();
private:
	//����owner��start_time����ά��Ϊ3��һ���ɾ����shard,�����µ�shard��id
	int CreateNewShard(string owner, tm start_time);

	fstream db_io_;
	string path_;
	int shard_count_;

	int max_id_;
	unordered_map<string, vector<ShardInfo>> DatabaseShardMap_;
};