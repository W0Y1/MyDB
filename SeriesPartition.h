#pragma once
#include"SeriesSegement.h"
#include"SeriesIndexToSegement.h"


class SeriesPartition {
public:
	SeriesPartition();

	//需要初始化index
	SeriesPartition(string path,int id);

	~SeriesPartition();

	void Init();

	void SetPath(string path);

	void SetId(int id) { id_ = id; }

	//需要同时插入segement和index
	SeriesId Insert(SeriesKey& seriekey);

	//根据id到索引中查找对应的offset后，再去Segement中查找
	//输出参数:SeriesKey
	void SearchSeriesKey(vector<SeriesKey>& serieskeys, SeriesId seriesid);

	//根据SeriesKey在索引中查找对应的id
	bool SearchSeriesId(SeriesKey& serieskey, SeriesId& seriesid);

	void FlushIndex();
private:
	fstream db_io_;
	bool is_dirty_;

	int id_;
	string path_;
	int segements_num_;						//char[2]
	uint64_t seq;									//char[3]

	SeriesSegement* segements;
	SeriesIndexToSegement* index;
};
