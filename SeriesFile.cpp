#include"SeriesFile.h"
#include<thread>
#include<functional>
#include<direct.h>

uint64_t DJBHash(const char* str) {
	uint64_t hash = 5381;

	while (*str) {
		hash += (hash << 5) + (*str++);
	}

	return (hash & 0x7FFFFFFF);
}

SeriesFile::SeriesFile(string path) {
	path_ = path;
	_mkdir(path_.c_str());
	partitions = new SeriesPartition[SERIES_PARTITION_NUM];
	for (int i = 0; i < SERIES_PARTITION_NUM; i++) {
		partitions[i].SetId(i);
		partitions[i].SetPath(path_ + "\\partition" + to_string(i));
		partitions[i].Init();
	}
}

SeriesFile::~SeriesFile() {
	delete[]partitions;
}

void SeriesFile::GetSeriesKey(vector<SeriesKey>& serieskeys, vector<SeriesId> seriesids) {
	//thread t1(bind(&SeriesFile::CreateThreadToSearch, this, ref(serieskeys), seriesids, 0, seriesids.size() / 2));
	//thread t2(bind(&SeriesFile::CreateThreadToSearch, this, ref(serieskeys), seriesids, seriesids.size() / 2, seriesids.size()));
	//t1.join();
	//t2.join();
	int num = (seriesids[0]) % SERIES_PARTITION_NUM;
	partitions[num].SearchSeriesKey(serieskeys, seriesids[0]);
}

SeriesId SeriesFile::IfExist(SeriesKey& serieskey) {
	string key = serieskey.ToString();
	int num = DJBHash(key.c_str()) % SERIES_PARTITION_NUM;
	SeriesId result;
	if (!partitions[num].SearchSeriesId(serieskey, result)) {
		return Insert(serieskey,num);
	}
	return result;
}

SeriesId SeriesFile::Insert(SeriesKey& serieskey, int index) {
	return partitions[index].Insert(serieskey);
}


void SeriesFile::CreateThreadToSearch(vector<SeriesKey>& serieskeys, vector<SeriesId> seriesids, int low, int high) {
	for (int i = low; i < high; i++) {
		int num = (seriesids[i]) % SERIES_PARTITION_NUM;
		partitions[num].SearchSeriesKey(serieskeys, seriesids[i]);
	}
}

void SeriesFile::Flush() {
	for (int i = 0; i < SERIES_PARTITION_NUM; i++) {
		partitions[i].FlushIndex();
	}
}