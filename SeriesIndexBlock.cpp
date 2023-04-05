#include"SeriesIndexBlock.h"

SeriesIndexBlock::SeriesIndexBlock() {
	file_trailer_.count_ = 0;
}

SeriesIndexBlock::SeriesIndexBlock(const Key& key, const char type[8]) {
	file_trailer_.key_ = key;
	memcpy(file_trailer_.type_, type, 8);
	file_trailer_.count_ = 0;
}

void SeriesIndexBlock::init(char data[4096]) {
	char tmp_2[3]={};
	memcpy(tmp_2, data + 4094, 2);
	this->file_trailer_.count_ = atof(tmp_2);
	memcpy(this->file_trailer_.type_, data + 4086, 8);

	memcpy(this->file_trailer_.key_.field_key_, data + 4070, 16);
	memcpy(tmp_2, data + 4068, 2);
	this->file_trailer_.key_.series_key_.tag_num_ = atof(tmp_2);
	int offset = 0;
	for (int i = file_trailer_.key_.series_key_.tag_num_ - 1; i >= 0; i--) {
		file_trailer_.key_.series_key_.tags_.push_back(Tag());
		//¸³Öµtagvalue
		offset += 2;
		memcpy(tmp_2, data + 4068 - offset, 2);
		file_trailer_.key_.series_key_.tags_[i].tagvalue_length_ = atof(tmp_2);
		offset += file_trailer_.key_.series_key_.tags_[i].tagvalue_length_;
		char* tmp = new char[file_trailer_.key_.series_key_.tags_[i].tagvalue_length_ + 1];
		memset(tmp, 0, file_trailer_.key_.series_key_.tags_[i].tagvalue_length_ + 1);
		memcpy(tmp, data + 4068 - offset, file_trailer_.key_.series_key_.tags_[i].tagvalue_length_);
		file_trailer_.key_.series_key_.tags_[i].tagvalue_ = tmp;
		delete[]tmp;
		//¸³Öµtagkey
		offset += 2;
		memcpy(tmp_2, data + 4068 - offset, 2);
		file_trailer_.key_.series_key_.tags_[i].tagkey_length_ = atof(tmp_2);
		offset += file_trailer_.key_.series_key_.tags_[i].tagkey_length_;
		char* new_tmp = new char[file_trailer_.key_.series_key_.tags_[i].tagkey_length_ + 1];
		memset(new_tmp, 0, file_trailer_.key_.series_key_.tags_[i].tagkey_length_ + 1);
		memcpy(new_tmp, data + 4068 - offset, file_trailer_.key_.series_key_.tags_[i].tagkey_length_);
		file_trailer_.key_.series_key_.tags_[i].tagkey_ = new_tmp;
		delete[]new_tmp;
	}
	offset += 2;
	memcpy(tmp_2, data + 4068 - offset, 2);
	file_trailer_.key_.series_key_.name_length_ = atof(tmp_2);
	offset += file_trailer_.key_.series_key_.name_length_;
	char tmp_16[17] = {};
	memcpy(tmp_16, data + 4068 - offset, file_trailer_.key_.series_key_.name_length_);
	file_trailer_.key_.series_key_.name_ = tmp_16;

	for (int i = 0; i < file_trailer_.count_; i++) {
		index_entrys_.push_back(IndexEntry());
		char tmp_8[8] = {};
		char tmp_16[16] = {};

		memcpy(tmp_16, data + i * 48, 16);
		this->index_entrys_[i].max_time_ = atof(tmp_16);
		memcpy(tmp_16, data + 16 + i * 48, 16);
		this->index_entrys_[i].min_time_ = atof(tmp_16);
		memcpy(tmp_8, data + 32 + i * 48, 8);
		this->index_entrys_[i].offset_ = atof(tmp_8);
		memcpy(tmp_8, data + 40 + i * 48, 8);
		this->index_entrys_[i].size_ = atof(tmp_8);
	}
	
}

void SeriesIndexBlock::AddNewIndexEntry(uint64_t min_time, uint64_t max_time, uint32_t size, uint32_t offset) {
	int index = this->file_trailer_.count_;
	index_entrys_.push_back(IndexEntry());
	index_entrys_[index].max_time_ = max_time;
	index_entrys_[index].min_time_ = min_time;
	index_entrys_[index].offset_ = offset;
	if (file_trailer_.type_ == "bool") index_entrys_[index].size_ = 28;
	else if (file_trailer_.type_ == "int") index_entrys_[index].size_ = 39;
	else if(file_trailer_.type_ == "double") index_entrys_[index].size_ = 43;
	else index_entrys_[index].size_ = size;

	file_trailer_.count_++;
}

void SeriesIndexBlock::Update(uint64_t min_time, uint64_t max_time, uint32_t change_size, uint32_t pos_entry) {
	index_entrys_[pos_entry].max_time_ = max_time;
	index_entrys_[pos_entry].min_time_ = min_time;
	if (file_trailer_.type_ == "bool") index_entrys_[pos_entry].size_ += 17;
	else if (file_trailer_.type_ == "int") index_entrys_[pos_entry].size_ = 28;
	else if (file_trailer_.type_ == "double") index_entrys_[pos_entry].size_ = 32;
	else index_entrys_[pos_entry].size_ += change_size;
}

bool SeriesIndexBlock::SearchDataBlock(uint64_t time,vector<pair<int, int>>& data) {
	for (int i = 0; i < file_trailer_.count_; i++) {
		if (time <= index_entrys_[i].max_time_ && time >= index_entrys_[i].min_time_) {
			data.push_back({ index_entrys_[i].offset_ , index_entrys_[i].size_ });
		}
	}
	if (data.size() == 0)return false;
	return true;
}

bool SeriesIndexBlock::SearchDataBlock(vector<pair<int, int>>& data) {
	if (file_trailer_.count_ == 0)return false;

	for (int i = 0; i < file_trailer_.count_; i++) {
		data.push_back({ index_entrys_[i].offset_ , index_entrys_[i].size_ });
	}
	return true;
}
