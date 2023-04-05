#pragma once
#include<unordered_map>
#include<list>
#include<mutex>
#include"TSIFile.h"
#include "config.h"
#include "SeriesKey.h"
#include"LRUReplacer.h"

using namespace std;

//将文件中不存在的SeriesKey存在缓冲池中
class BufferPool {
public:
	BufferPool(size_t pool_size);

	~BufferPool();

	bool LoadNewSeriesKey(SeriesKey& series_key,SeriesId seriesid, TSIFile* tsi);

	SeriesId GetSeriesId(int frame_ids);

	SeriesKey GetSeriesKey(SeriesId seriesid) { return series_keys_[seriesid]; }

	//num用于返回SeriesKey的数量	frame_ids用于返回对应serieskey的frame_id
	void FetchSeriesKey(string name, string tagkey, string tagvalue, vector<SeriesKey>& keys);

	//当数据库被关闭前调用
	//将所有SeriesKey以及对应的SeriesId写入到tsi文件中
	bool FlushAllSeriesKey(TSIFile* tsi);

	bool IsExist(string name, string tagkey, string tagvalue);

private:
	bool UnpinSeriesId(int frame_ids);

	bool find_replace(frame_id_t* frame_id, TSIFile* tsi);

	void erase_table(string name, string tagkey, string tagvalue, frame_id_t frame_id);

	//name->tagKey->tagValue->seriesID
	unordered_map < string, unordered_map<string, unordered_map<string, list<frame_id_t>>>> series_table_;			
	size_t pool_size_;
	SeriesId* seriesids_;																	//seriesId的队列	seriesids_[frame_id]
	unordered_map<SeriesId, SeriesKey> series_keys_;
	LRUReplacer* replacer_;
	list<frame_id_t> free_list_;														//存储seriesids_中空的位置

	mutex latch_;
};