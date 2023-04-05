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

	//id������shard��ͬ
	Engine(int id,string path, SeriesFile* seriesfile);

	~Engine();

	void SetPath(string path);

	void SetId(int id) { id_ = id; }

	//д������
	void WriteToCache(SeriesKey& serieskey, string field_key, string field_value, string value_type,uint64_t timestamp);
	
	//��Cache��С����ʱ����
	//���ݲ�ͬ��Key(SeriesKey+FieldKey)ˢ�µ�ͬһ�ļ� �γɲ�ͬ��SeriesDataBlock ��IndexEntry
	//datablock�е�����(����time)��datablock��indexblock(����key)����Ҫ˳������ ����Ѵ�����Ҫ׷��
	void WriteToFile(map<Key, Values>& data, string path);

	//��Cahce�е�����ˢ�µ�������
	void FlushToDisk();

	bool SearchData(SeriesKey serieskey, string field_key, vector<uint64_t>& times, vector<string>& values);

	bool SearchDataByTime(SeriesKey serieskey, string field_key, uint64_t timestamp, vector<string>& values);

private:
	//�����̸߳���low��high���ö�Ӧ����[low,high)��compact_
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