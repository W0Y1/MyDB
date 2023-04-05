#pragma once
#include<fstream>
#include<mutex>
#include"SeriesBlock.h"
#include"TagBlock.h"
#include"MeasurementBlock.h"

struct TSIFileTrailer {
	int id_;												//char[2]
	uint32_t sbk_offset_;						//char[8]
	uint32_t sbk_size_;							//char[8]
	uint32_t tbks_offset_;						//char[8]
	uint32_t tbks_size_;							//char[8]
	uint32_t mbk_offset_;						//char[8]
	uint32_t mbk_size_;							//char[8]
};

//ÿ���γ�һ���µ�TSIFile
class TSIFile {
public:
	//�½��ļ�
	TSIFile();

	//��ȡ�ļ���ʼ��
	TSIFile(string path, int id);

	~TSIFile();

	void SetPath(string path) { path_ = path; }

	void SetId(int id) { file_trailer_.id_ = id; }

	//��ȡ�ļ���ʼ��
	void Init();

	//ȷ���ļ����Ƿ��Ѿ���������serieskey������
	bool SearchSeriesKey(SeriesKey serieskey);

	//Ѱ�ҷ���������SeriesId
	vector<SeriesId> FindSeriesId(string name,string tag_key,string tag_value);

	//���ڴ��е��µ�SeriesKeyд�뵽�ļ���
	bool AddNewSeriesKey(SeriesKey serieskey, SeriesId seriesid);   

	bool FlushToFile();

private:
	mutex latch_;

	fstream db_io_;
	string path_;

	//���ݲ���
	SeriesBlock* sbk_;
	array<TagBlock, 20> tbks_;			//pos_tagblock = pos_measurements
	MeasurementBlock* mbk_;

	TSIFileTrailer file_trailer_;
};