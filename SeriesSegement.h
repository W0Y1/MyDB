#pragma once
#include"SeriesKey.h"
#include"config.h"
#include<fstream>

//映射一个文件
class SeriesSegement {
public:
	SeriesSegement();

	SeriesSegement(int id, string path);

	~SeriesSegement();

	void SetId(int id);

	int GetSegementId();

	//判断文件大小 若超出大小则返回0
	//生成SeriesId，返回SeriesOffset
	SeriesOffset InsertNewData(SeriesKey serieskey,uint16_t id);

	//返回值SeriesKey,如果不存在返回false
	//根据offset值直接读取文件内容
	bool GetSeriesKey(vector<SeriesKey>& serieskeys,SeriesOffset seriesoffset);

private:
	fstream db_io_;
	int id_;
	string path_;
	uint16_t num_;
	uint32_t size_;																							//最大4MB,超过4MB创建下一个Segement
};