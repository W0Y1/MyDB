#pragma once
#include"SeriesSegement.h"
#include"SeriesIndexToSegement.h"


class SeriesPartition {
public:
	SeriesPartition();

	//��Ҫ��ʼ��index
	SeriesPartition(string path,int id);

	~SeriesPartition();

	void Init();

	void SetPath(string path);

	void SetId(int id) { id_ = id; }

	//��Ҫͬʱ����segement��index
	SeriesId Insert(SeriesKey& seriekey);

	//����id�������в��Ҷ�Ӧ��offset����ȥSegement�в���
	//�������:SeriesKey
	void SearchSeriesKey(vector<SeriesKey>& serieskeys, SeriesId seriesid);

	//����SeriesKey�������в��Ҷ�Ӧ��id
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
