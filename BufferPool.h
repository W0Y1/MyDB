#pragma once
#include<unordered_map>
#include<list>
#include<mutex>
#include"TSIFile.h"
#include "config.h"
#include "SeriesKey.h"
#include"LRUReplacer.h"

using namespace std;

//���ļ��в����ڵ�SeriesKey���ڻ������
class BufferPool {
public:
	BufferPool(size_t pool_size);

	~BufferPool();

	bool LoadNewSeriesKey(SeriesKey& series_key,SeriesId seriesid, TSIFile* tsi);

	SeriesId GetSeriesId(int frame_ids);

	SeriesKey GetSeriesKey(SeriesId seriesid) { return series_keys_[seriesid]; }

	//num���ڷ���SeriesKey������	frame_ids���ڷ��ض�Ӧserieskey��frame_id
	void FetchSeriesKey(string name, string tagkey, string tagvalue, vector<SeriesKey>& keys);

	//�����ݿⱻ�ر�ǰ����
	//������SeriesKey�Լ���Ӧ��SeriesIdд�뵽tsi�ļ���
	bool FlushAllSeriesKey(TSIFile* tsi);

	bool IsExist(string name, string tagkey, string tagvalue);

private:
	bool UnpinSeriesId(int frame_ids);

	bool find_replace(frame_id_t* frame_id, TSIFile* tsi);

	void erase_table(string name, string tagkey, string tagvalue, frame_id_t frame_id);

	//name->tagKey->tagValue->seriesID
	unordered_map < string, unordered_map<string, unordered_map<string, list<frame_id_t>>>> series_table_;			
	size_t pool_size_;
	SeriesId* seriesids_;																	//seriesId�Ķ���	seriesids_[frame_id]
	unordered_map<SeriesId, SeriesKey> series_keys_;
	LRUReplacer* replacer_;
	list<frame_id_t> free_list_;														//�洢seriesids_�пյ�λ��

	mutex latch_;
};