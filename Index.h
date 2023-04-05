#pragma once
#include<string>
#include<direct.h>
#include "SeriesFile.h"
#include "BufferPool.h"
#include "TSIFile.h"
#include"helper.h"

using namespace std;

class Index {
public:
	Index(SeriesFile* sfile);

	Index(string path,string database,SeriesFile* sfile);

	~Index();

	void SetPath(string path);

	void SetDatabase(string database) { database_ = database; }

	//查询功能 返回serieskey的集合 查询bufferpool和tsi文件中
	vector<SeriesKey> SearchSeriesKey(string name,string tag_key,string tag_value);

	//查找(先bufferpool后tsi文件)是否存在该serieskey，若不存在向bufferpool和seriesfile添加新serieskey
	bool IsExistSeriesKey(SeriesKey& serieskey);

	bool FlushToDisk();
	
private:
	//创建线程去对应区间[low,high)的tsi文件中去找对应的seriesid 返回seriesids
	void CreateThreadToSearch(string name, string tag_key, string tag_value, int low, int high,vector<SeriesId>& seriesids);

	//创建线程去对应区间[low,high)的tsi文件中去判断对应的SeriesKey是否存在
	void CreateThreadToJudge(SeriesKey& serieskey, int low, int high, bool& flag);

	//被IsExistSeriesKey调用
	void AddNewSeriesKey(SeriesKey& serieskey);

	string path_;
	string database_;
	int tsi_num_;

	SeriesFile* sfile_;

	TSIFile* tsi_;
	BufferPool* bufferpool_;
};