#pragma once
#include"SeriesPartition.h"

//算作元数据部分
class SeriesFile {
public:
	SeriesFile(string path);

	~SeriesFile();

	//根据seriesids  返回serieskey 结果向其push_back
	void GetSeriesKey(vector<SeriesKey>& serieskeys,vector<SeriesId> seriesids);
	
	//如果指定的serieskey不存在则调用insert，存在则直接返回id
	SeriesId IfExist(SeriesKey& serieskey);

	void Flush();

private:
	//向指定index的partitions插入serieskey,返回插入到sfile中返回的seriesid
	SeriesId Insert(SeriesKey& serieskey, int index);

	//创建线程在调用该函数，在指定区间内[low,high)查找对应的serieskey并push_back
	void CreateThreadToSearch(vector<SeriesKey>& serieskeys, vector<SeriesId> seriesids, int low, int high);

	string path_;
	SeriesPartition* partitions;
};
