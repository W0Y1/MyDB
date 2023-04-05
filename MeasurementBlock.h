#pragma once
#include<unordered_map>
#include<string>
#include<array>
using namespace std;

struct Measurement {
	string name_;													//char[16]
	uint32_t tag_block_offset_;								//char[8]
	uint32_t tag_block_size_;									//char[8]

	Measurement() {
		tag_block_offset_ = 0;
		tag_block_size_ = 0;
	}
};

struct MeasurementBlockTrailer {
	uint32_t data_offset_;										//char[8]
	uint32_t data_size_;											//char[8]
	uint32_t hash_offset_;										//char[8]
	uint32_t hash_size_;											//char[8]
};

class MeasurementBlock {
public:
	friend class TSIFile;
	//����BlockTrailer���������
	MeasurementBlock();

	void Init(char* data, int size);

	//�������� ���measurement���������½�һ�� ���� pos_measurements ��tagblock��offset
	//����flag==0�������Ѿ����ڵ�measurement ��֮
	int Insert(string name, int* tagblock_offset, int* flag);

	//�������:name��Ӧmeasurement 
	//��������ָ��Ϊmeasurement��Ӧ��tagblock����Ϣ
	bool SearchTagBlock(string name,uint32_t* tag_block_offset, uint32_t* tag_block_size);

	//��TagBlock�����ı�ʱ��Ҫ���� ��Ҫ����Measurement������MeasurementҲ����
	//�������:measurementΪ�ı��TagBlock����Ӧ��
	void UpdateMeasurement(int tar_index ,uint32_t tag_block_offset,uint32_t tag_block_size);

	void UpdateBlockTrailer(int change_size);

	//������offset��Ϣ���� ������hash_index��size
	int UpdateOffset(int change_size);

private:
	array<Measurement,100> measurements_;
	unordered_map<string, uint16_t> hash_index_;										//name->pos_Measurement				char[8]->char[2]
	MeasurementBlockTrailer block_trailer_;
};
