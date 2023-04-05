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

	//��ѯ���� ����serieskey�ļ��� ��ѯbufferpool��tsi�ļ���
	vector<SeriesKey> SearchSeriesKey(string name,string tag_key,string tag_value);

	//����(��bufferpool��tsi�ļ�)�Ƿ���ڸ�serieskey������������bufferpool��seriesfile�����serieskey
	bool IsExistSeriesKey(SeriesKey& serieskey);

	bool FlushToDisk();
	
private:
	//�����߳�ȥ��Ӧ����[low,high)��tsi�ļ���ȥ�Ҷ�Ӧ��seriesid ����seriesids
	void CreateThreadToSearch(string name, string tag_key, string tag_value, int low, int high,vector<SeriesId>& seriesids);

	//�����߳�ȥ��Ӧ����[low,high)��tsi�ļ���ȥ�ж϶�Ӧ��SeriesKey�Ƿ����
	void CreateThreadToJudge(SeriesKey& serieskey, int low, int high, bool& flag);

	//��IsExistSeriesKey����
	void AddNewSeriesKey(SeriesKey& serieskey);

	string path_;
	string database_;
	int tsi_num_;

	SeriesFile* sfile_;

	TSIFile* tsi_;
	BufferPool* bufferpool_;
};