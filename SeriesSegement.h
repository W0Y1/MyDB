#pragma once
#include"SeriesKey.h"
#include"config.h"
#include<fstream>

//ӳ��һ���ļ�
class SeriesSegement {
public:
	SeriesSegement();

	SeriesSegement(int id, string path);

	~SeriesSegement();

	void SetId(int id);

	int GetSegementId();

	//�ж��ļ���С ��������С�򷵻�0
	//����SeriesId������SeriesOffset
	SeriesOffset InsertNewData(SeriesKey serieskey,uint16_t id);

	//����ֵSeriesKey,��������ڷ���false
	//����offsetֱֵ�Ӷ�ȡ�ļ�����
	bool GetSeriesKey(vector<SeriesKey>& serieskeys,SeriesOffset seriesoffset);

private:
	fstream db_io_;
	int id_;
	string path_;
	uint16_t num_;
	uint32_t size_;																							//���4MB,����4MB������һ��Segement
};