#include"SeriesSegement.h"

SeriesSegement::SeriesSegement() {
	id_ = INVALID_ID;
	num_ = 0;
	size_ = 0;
}

SeriesSegement::SeriesSegement(int id, string path) {
	id_ = id;
	path_ = path;
	num_ = 0;
	size_ = 0;
}

SeriesSegement::~SeriesSegement() {
}

void SeriesSegement::SetId(int id) {
	id_ = id;
}

int SeriesSegement::GetSegementId() {
	return id_;
}

SeriesOffset SeriesSegement::InsertNewData(SeriesKey serieskey, uint16_t id) {
	db_io_.open(path_, ios::out | ios::binary);
	db_io_.seekg(0, ios::end);
	size_ = db_io_.tellg();
	if (size_ >= SEGEMENT_SIZE) return 0;
	uint64_t offset = ((long long)id_ << 32) | (size_&0xFFFFFFFF);
	db_io_.seekg(size_);
	char tmp_4[5] = {};
	memcpy(tmp_4, to_string(id).c_str(), 4);
	db_io_.write(tmp_4, 4);

	char* tmp = new char[serieskey.GetSize() + 1];
	int offset_tmp = 0;

	memcpy(tmp + offset_tmp, to_string(serieskey.GetName().length()).c_str(), 2);
	offset_tmp += 2;

	memcpy(tmp + offset_tmp, serieskey.GetName().c_str(), serieskey.GetName().length());
	offset_tmp += serieskey.GetName().length();

	memcpy(tmp + offset_tmp, to_string(serieskey.GetNum()).c_str(), 2);
	offset_tmp += 2;

	for (int i = 0; i < serieskey.GetNum(); i++) {
		Tag tag = serieskey.GetTag(i);

		memcpy(tmp + offset_tmp, to_string(tag.tagkey_length_).c_str(), 2);
		offset_tmp += 2;

		memcpy(tmp + offset_tmp, tag.tagkey_.c_str(), tag.tagkey_length_);
		offset_tmp += tag.tagkey_length_;

		memcpy(tmp + offset_tmp, to_string(tag.tagvalue_length_).c_str(), 2);
		offset_tmp += 2;

		memcpy(tmp + offset_tmp, tag.tagvalue_.c_str(), tag.tagvalue_length_);
		offset_tmp += tag.tagvalue_length_;
	}

	db_io_.write(tmp, serieskey.GetSize());
	db_io_.close();

	size_ += serieskey.GetSize() + 4;
	return offset;
}

bool SeriesSegement::GetSeriesKey(vector<SeriesKey>& serieskeys, SeriesOffset seriesoffset) {
	db_io_.open(path_, ios::in | ios::binary);
	if (!db_io_.is_open()) return false;

	int offset = (int)seriesoffset;
	offset += 4;
	db_io_.seekg(offset);

	char tmp_2[3]={};
	db_io_.seekg(offset);
	db_io_.read(tmp_2, 2);
	SeriesKey serieskey;
	serieskey.name_length_ = atof(tmp_2);

	char buffer[16]={};
	db_io_.read(buffer, serieskey.name_length_);
	serieskey.name_ = buffer;

	db_io_.read(tmp_2, 2);
	serieskey.tag_num_ = atof(tmp_2);

	for (int i = 0; i < serieskey.tag_num_; i++) {
		db_io_.read(tmp_2, 2);
		serieskey.tags_.push_back(Tag());
		serieskey.tags_[i].tagkey_length_ = atof(tmp_2);

		db_io_.read(buffer, serieskey.tags_[i].tagkey_length_);
		serieskey.tags_[i].tagkey_ = buffer;

		db_io_.read(tmp_2, 2);
		serieskey.tags_[i].tagvalue_length_ = atof(tmp_2);

		db_io_.read(buffer, serieskey.tags_[i].tagvalue_length_);
		serieskey.tags_[i].tagvalue_ = buffer;
	}
	db_io_.close();
	serieskeys.push_back(serieskey);

	return true;
}