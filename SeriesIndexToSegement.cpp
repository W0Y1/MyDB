#include"SeriesIndexToSegement.h"

SeriesIndexToSegement::SeriesIndexToSegement() {
	num = 0;
}

SeriesIndexToSegement::SeriesIndexToSegement(string path) {
	path_ = path;
	num = 0;
}

bool SeriesIndexToSegement::Init() {
	db_io_.open(path_, ios::in | ios::binary);
	if (!db_io_.is_open()) return false;

	char tmp_3[4] = {};

	db_io_.read(tmp_3, 3);
	num = atof(tmp_3);

	for (int i = 0; i < num; i++) {
		char tmp_2[3] = {};
		SeriesKey serieskey;

		db_io_.read(tmp_2, 2);
		serieskey.name_length_ = atof(tmp_2);
		char buffer[17] = {};
		db_io_.read(buffer, serieskey.name_length_);
		serieskey.name_ = buffer;

		db_io_.read(tmp_2, 2);
		serieskey.tag_num_ = atof(tmp_2);
		for (int i = 0; i < serieskey.tag_num_; i++) {
			Tag new_tag;
			serieskey.tags_.push_back(new_tag);
			db_io_.read(tmp_2, 2);
			serieskey.tags_[i].tagkey_length_ = atof(tmp_2);
			db_io_.read(buffer, serieskey.tags_[i].tagkey_length_);
			serieskey.tags_[i].tagkey_ = buffer;
			db_io_.read(tmp_2, 2);
			serieskey.tags_[i].tagvalue_length_ = atof(tmp_2);
			db_io_.read(buffer, serieskey.tags_[i].tagvalue_length_);
			serieskey.tags_[i].tagvalue_ = buffer;
		}
		char tmp_4[5] = {};
		db_io_.read(tmp_4, 4);
		SeriesId seriesid = atof(tmp_4);

		KeyIdMap[serieskey] = seriesid;
	}

	for (int i = 0; i < num; i++) {
		char tmp_4[5] = {};
		db_io_.read(tmp_4, 4);
		SeriesId seriesid = atof(tmp_4);
		char tmp_12[13] = {};
		db_io_.read(tmp_12, 12);
		SeriesOffset seriesoffset = atof(tmp_12);

		IdOffsetMap[seriesid] = seriesoffset;
	}
	db_io_.close();

	return true;
}

void SeriesIndexToSegement::InsertIntoIndex(SeriesKey& serieskey, SeriesId seriesid, SeriesOffset seriesoffset) {
	if (KeyIdMap.count(serieskey) != 0)return;

	KeyIdMap[serieskey] = seriesid;
	IdOffsetMap[seriesid] = seriesoffset;
	num++;
}

bool SeriesIndexToSegement::GetSeriesOffset(SeriesId seriesid, SeriesOffset* seriesoffset) {
	if (IdOffsetMap.count(seriesid) == 0) return false;

	*seriesoffset = IdOffsetMap[seriesid];
	return true;
}

bool SeriesIndexToSegement::Flush() {
	db_io_.open(path_, ios::out | ios::binary | ios::in);
	if (!db_io_.is_open()) {
		db_io_.open(path_, ios::out | ios::binary);
	}

	char tmp_3[4] = {};
	memcpy(tmp_3, to_string(num).c_str(), 3);
	db_io_.write(tmp_3, 3);

	for (auto iter = KeyIdMap.begin(); iter != KeyIdMap.end(); iter++) {
		SeriesKey serieskey = iter->first;
		SeriesId seriesid = iter->second;
		char tmp_2[3] = {};

		memcpy(tmp_2, to_string(serieskey.GetName().length()).c_str(), 2);
		db_io_.write(tmp_2, 2);

		db_io_.write(serieskey.GetName().c_str(), serieskey.GetName().length());

		memcpy(tmp_2, to_string(serieskey.GetNum()).c_str(), 2);
		db_io_.write(tmp_2, 2);

		for (int i = 0; i < serieskey.GetNum(); i++) {
			Tag tag = serieskey.GetTag(i);
			memcpy(tmp_2, to_string(tag.tagkey_length_).c_str(), 2);
			db_io_.write(tmp_2, 2);
			db_io_.write(tag.tagkey_.c_str(), tag.tagkey_length_);

			memcpy(tmp_2, to_string(tag.tagvalue_length_).c_str(), 2);
			db_io_.write(tmp_2, 2);
			db_io_.write(tag.tagvalue_.c_str(), tag.tagvalue_length_);
		}

		char tmp_4[5] = {};
		memcpy(tmp_4, to_string(seriesid).c_str(), 4);
		db_io_.write(tmp_4, 4);
	}

	for (auto iter = IdOffsetMap.begin(); iter != IdOffsetMap.end(); iter++) {
		SeriesId seriesid = iter->first;
		SeriesOffset seriesoffset = iter->second;
		char tmp_4[5] = {};
		char tmp_12[13] = {};

		memcpy(tmp_4, to_string(seriesid).c_str(), 4);
		memcpy(tmp_12, to_string(seriesoffset).c_str(), 12);
		db_io_.write(tmp_4, 4);
		db_io_.write(tmp_12, 12);
	}
	db_io_.close();

	return true;
}

bool SeriesIndexToSegement::GetSeriesId(SeriesKey& serieskey, SeriesId& seriesid) {
	if (KeyIdMap.count(serieskey) == 0)return false;
	seriesid = KeyIdMap[serieskey];
	return true;
}