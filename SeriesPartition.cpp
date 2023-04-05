#include"SeriesPartition.h"
#include<direct.h>

SeriesPartition::SeriesPartition() {
	id_ = -1;
	seq = 0;
	segements_num_ = 0;
	index = new SeriesIndexToSegement;
	is_dirty_ = false;
}

SeriesPartition::SeriesPartition(string path, int id) {
	id_ = id;
	path_ = path;
	seq = id_ + 1;
	segements_num_ = 0;
	index = new SeriesIndexToSegement(path + "\\index.txt");
	index->Init();
	is_dirty_ = false;
}

SeriesPartition::~SeriesPartition() {
	delete index;
}

void SeriesPartition::Init() {
	string path = path_ + "\\meta.txt";
	db_io_.open(path, ios::in | ios::binary);
	if (!db_io_.is_open())	return;

	char tmp_3[4] = {};
	char tmp_2[3] = {};
	db_io_.read(tmp_2, 2);
	segements_num_ = atof(tmp_2);
	db_io_.read(tmp_3, 3);
	seq = atof(tmp_3);
	db_io_.close();
}

void SeriesPartition::SetPath(string path) {
	path_ = path;
	_mkdir(path_.c_str());
	_mkdir((path_ + "\\segements").c_str());
	index->SetPath(path_ + "\\index.txt");
	index->Init();
}

SeriesId SeriesPartition::Insert(SeriesKey& seriekey) {
	SeriesId serieisid = seq;
	seq += SERIES_PARTITION_NUM;
	
	if (segements_num_ == 0)segements_num_++;
	segements = new SeriesSegement(segements_num_, path_ + "\\segements\\segement"+to_string(segements_num_) + ".txt");
	SeriesOffset seriesoffset = segements->InsertNewData(seriekey, serieisid);

	if (seriesoffset == 0) {
		segements_num_++;
		segements = new SeriesSegement(segements_num_, path_ + "\\segements\\segement" + to_string(segements_num_) + ".txt");
		seriesoffset = segements->InsertNewData(seriekey, serieisid);
	}
	index->InsertIntoIndex(seriekey, serieisid, seriesoffset);
	is_dirty_ = true;
	delete segements;
	return serieisid;
}

void SeriesPartition::SearchSeriesKey(vector<SeriesKey>& serieskeys, SeriesId seriesid) {
	SeriesOffset seriesoffset = 0;
	if (!index->GetSeriesOffset(seriesid, &seriesoffset)) return ;

	int id = seriesoffset >> 32;
	segements = new SeriesSegement(id, path_ + "\\segements\\segement" + to_string(id) + ".txt");
	segements->GetSeriesKey(serieskeys, seriesoffset);
	delete segements;
}

bool SeriesPartition::SearchSeriesId(SeriesKey& serieskey, SeriesId& seriesid) {
	return index->GetSeriesId(serieskey, seriesid);
}

void SeriesPartition::FlushIndex() { 
	if (!is_dirty_)return;
	string path = path_ + "\\meta.txt";
	db_io_.open(path, ios::out | ios::binary | ios::in);
	if (!db_io_.is_open()) {
		db_io_.open(path, ios::out | ios::binary);
	}
	char tmp_2[3] = {};
	char tmp_3[4] = {};
	memcpy(tmp_3, to_string(seq).c_str(), 3);
	memcpy(tmp_2, to_string(segements_num_).c_str(), 2);
	db_io_.write(tmp_2, 2);
	db_io_.write(tmp_3, 3);
	db_io_.close();
	index->Flush(); 
}