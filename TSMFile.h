#pragma once
#include<fstream>
#include"SeriesDataBlock.h"
#include"SeriesIndexBlock.h"

//ÿ��Cacheˢ���γ�һ���µ�TSM�ļ�
class TSMFile {
public:
	TSMFile();

	TSMFile(string path);

	~TSMFile();

	void SetPath(string path);

	//����key���ֲ����Ƿ����indexblock
	//1.1��������ȥ�����һ��indexentry����count���ҵ�datablock
	//1.2��ȡ����datablock,���º���index��ص�offset ���datablock��С����,���½�һ��datablock��indexentryд�뵽ԭblock��
	//2.�������� ���½�indexblock��datablock(��Ҫ˳��)
	bool InsertData(const Key& key, string field_value,uint64_t timestamp, const char type[8]);

	//����ʱ�������value
	bool SearchData(Key& key, uint64_t timestamp, vector<string>& values);

	//��������ʱ�����value
	bool SearchData(Key& key, vector<uint64_t>& times, vector<string>& values);

	//�ڱ��ر�ʱ����,������ˢ��������
	void FlushToFile();

private:
	//ͨ��key���ֲ���Ѱ��indexblock ����indexblock�±�
	//���С����Сkey����-1 ����������key����100 �����ǰ������indexblock����-100
	int SearchIndexBlock(const Key& key);

	//����ǰkey������tsm�ļ�ʱ����
	//flag==0��index��߲����µ�block flag==1���ұ߲����µ�block
	//�����ڴ��е�sibs_
	void NewIndexBlock(const Key& key, const char type[8],int index,bool flag);

	fstream db_io_;

	string path_;

	//���ݲ���
	vector<SeriesDataBlock*>sdbs_;
	vector<SeriesIndexBlock*> sibs_;

	uint32_t index_offset_;																	//char[8]
	uint32_t index_size_;																	//char[8]
};
