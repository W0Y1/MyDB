#pragma once
#include"SeriesKey.h"
#include<fstream>
#include<map>

using std::map;

class SeriesIndexToSegement
{
public:
	SeriesIndexToSegement();

	SeriesIndexToSegement(string path);

	void SetPath(string path) { path_ = path; }

	bool Init();

	void InsertIntoIndex(SeriesKey& serieskey,SeriesId seriesid,SeriesOffset seriesoffset );

	//����SeriesId��ȡSeriesOffset,�������ڷ���false,����ֵSeriesOffset
	bool GetSeriesOffset(SeriesId seriesid,SeriesOffset* seriesoffset);

	//����serieskey��ȡseriesid
	bool GetSeriesId(SeriesKey& serieskey, SeriesId& seriesid);

	bool Flush();
private:
	string path_;
	fstream db_io_;

	int num;																						//char[3]
	map<SeriesKey, SeriesId> KeyIdMap;
	unordered_map<SeriesId, SeriesOffset> IdOffsetMap;
};