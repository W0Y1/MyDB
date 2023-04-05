#pragma once
#include"SeriesKey.h"
#include"Cache.h"
#include"Compact.h"
#include"SeriesFile.h"
#include"helper.h"
#include<direct.h>
#include <vector>
#include<functional>

class Engine {
public:
	Engine(SeriesFile* seriesfile);

	//id与所属shard相同
	Engine(int id,string path, SeriesFile* seriesfile);

	~Engine();

	void SetPath(string path);

	void SetId(int id) { id_ = id; }

	//写入数据
	void WriteToCache(SeriesKey& serieskey, string field_key, string field_value, string value_type,uint64_t timestamp);
	
	//当Cache大小过大时调用
	//根据不同的Key(SeriesKey+FieldKey)刷新到同一文件 形成不同的SeriesDataBlock 和IndexEntry
	//datablock中的数据(根据time)与datablock和indexblock(根据key)都需要顺序排序 如果已存在需要追加
	void WriteToFile(map<Key, Values>& data, string path);

	//将Cahce中的数据刷新到磁盘中
	void FlushToDisk();

	bool SearchData(SeriesKey serieskey, string field_key, vector<uint64_t>& times, vector<string>& values);

	bool SearchDataByTime(SeriesKey serieskey, string field_key, uint64_t timestamp, vector<string>& values);

private:
	//创建线程根据low和high调用对应区间[low,high)的compact_
	void CreatTheadToSearchByTime(Key key, uint64_t timestamp, vector<string>& values, int low, int high, bool* flag);
	void CreatTheadToSearch(Key key, vector<uint64_t>& times, vector<string>& values, int low, int high, bool* flag);

	int id_;
	string path_;
	int file_num_;
	vector<string> tsm_files_;

	vector<Compact*> compact_;
	SeriesFile* sfile_;
	Cache* cache_;
};