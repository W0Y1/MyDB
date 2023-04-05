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

	//从meta文件中读取数据,并将时间到期的shard删除
	bool Init();

	//根据database和time返回对应shard的id值,并将新生成的shard刷新到磁盘中
	//flag为0是被查询调用 为1是被插入调用
	int GetShard(string database, tm time, bool flag);

	//返回指定database下的所有shard 
	void GetShard(string database, vector<int>& ids);

	unordered_map<string, vector<ShardInfo>> GetAllShard() {return DatabaseShardMap_;}

	vector<ShardInfo> GetAllShardInfo();
private:
	//根据owner和start_time创建维度为3天一年后删除的shard,返回新的shard的id
	int CreateNewShard(string owner, tm start_time);

	fstream db_io_;
	string path_;
	int shard_count_;

	int max_id_;
	unordered_map<string, vector<ShardInfo>> DatabaseShardMap_;
};