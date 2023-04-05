#pragma once
#include<fstream>
#include"SeriesDataBlock.h"
#include"SeriesIndexBlock.h"

//每次Cache刷新形成一个新的TSM文件
class TSMFile {
public:
	TSMFile();

	TSMFile(string path);

	~TSMFile();

	void SetPath(string path);

	//根据key二分查找是否存在indexblock
	//1.1若存在则去找最后一个indexentry根据count数找到datablock
	//1.2读取更新datablock,更新后续index相关的offset 如果datablock大小过大,则新建一个datablock和indexentry写入到原block后
	//2.若不存在 则新建indexblock和datablock(需要顺序)
	bool InsertData(const Key& key, string field_value,uint64_t timestamp, const char type[8]);

	//根据时间戳返回value
	bool SearchData(Key& key, uint64_t timestamp, vector<string>& values);

	//返回所有时间戳和value
	bool SearchData(Key& key, vector<uint64_t>& times, vector<string>& values);

	//在被关闭时调用,将整个刷新至磁盘
	void FlushToFile();

private:
	//通过key二分查找寻找indexblock 返回indexblock下标
	//如果小于最小key返回-1 如果大于最大key返回100 如果当前不存在indexblock返回-100
	int SearchIndexBlock(const Key& key);

	//当当前key不存在tsm文件时调用
	//flag==0向index左边插入新的block flag==1向右边插入新的block
	//更改内存中的sibs_
	void NewIndexBlock(const Key& key, const char type[8],int index,bool flag);

	fstream db_io_;

	string path_;

	//数据部分
	vector<SeriesDataBlock*>sdbs_;
	vector<SeriesIndexBlock*> sibs_;

	uint32_t index_offset_;																	//char[8]
	uint32_t index_size_;																	//char[8]
};
