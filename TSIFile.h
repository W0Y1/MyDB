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

//每次形成一个新的TSIFile
class TSIFile {
public:
	//新建文件
	TSIFile();

	//读取文件初始化
	TSIFile(string path, int id);

	~TSIFile();

	void SetPath(string path) { path_ = path; }

	void SetId(int id) { file_trailer_.id_ = id; }

	//读取文件初始化
	void Init();

	//确认文件中是否已经构建过该serieskey的索引
	bool SearchSeriesKey(SeriesKey serieskey);

	//寻找符合条件的SeriesId
	vector<SeriesId> FindSeriesId(string name,string tag_key,string tag_value);

	//将内存中的新的SeriesKey写入到文件中
	bool AddNewSeriesKey(SeriesKey serieskey, SeriesId seriesid);   

	bool FlushToFile();

private:
	mutex latch_;

	fstream db_io_;
	string path_;

	//数据部分
	SeriesBlock* sbk_;
	array<TagBlock, 20> tbks_;			//pos_tagblock = pos_measurements
	MeasurementBlock* mbk_;

	TSIFileTrailer file_trailer_;
};