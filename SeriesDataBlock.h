#pragma once
#include"config.h"
#include<string>
#include<vector>
using namespace std;

class SeriesDataBlock {
public:
	friend class TSMFile;

	SeriesDataBlock();

	//�µ�DataBlock
	SeriesDataBlock(string type);

	void init(char* data, int size);

	//����������
	//length<500��ɼ�������
	bool InsertNewData(uint64_t timestamp, string value);

	//���ö��ֲ��Ҹ���ʱ���������
	bool SearchDataByTime(uint64_t timestamps, string* value);

	//����������ʱ����Լ�����
	bool GetData(vector<uint64_t>& timestamp, vector<string>& value);

	uint64_t GetMinTime() { return timestamps[0]; }

	uint64_t GetMaxTime() { return timestamps[length_ - 1]; }

	char* GetType() { return type_; }

private:
	//���ֲ��Ҹ���ʱ�������pos
	int BinarySearch(uint64_t timestamp);

	char type_[8];																	//��������
	int length_;																		//len(timestamp)	char[3]
	vector<uint64_t>timestamps;											//char[16]
	vector<string>values;														//���� int:char[12]   double:char[16] bool:char [1] string:char[32]
};
