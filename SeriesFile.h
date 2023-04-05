#pragma once
#include"SeriesPartition.h"

//����Ԫ���ݲ���
class SeriesFile {
public:
	SeriesFile(string path);

	~SeriesFile();

	//����seriesids  ����serieskey �������push_back
	void GetSeriesKey(vector<SeriesKey>& serieskeys,vector<SeriesId> seriesids);
	
	//���ָ����serieskey�����������insert��������ֱ�ӷ���id
	SeriesId IfExist(SeriesKey& serieskey);

	void Flush();

private:
	//��ָ��index��partitions����serieskey,���ز��뵽sfile�з��ص�seriesid
	SeriesId Insert(SeriesKey& serieskey, int index);

	//�����߳��ڵ��øú�������ָ��������[low,high)���Ҷ�Ӧ��serieskey��push_back
	void CreateThreadToSearch(vector<SeriesKey>& serieskeys, vector<SeriesId> seriesids, int low, int high);

	string path_;
	SeriesPartition* partitions;
};
