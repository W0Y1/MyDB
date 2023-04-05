#include"SeriesKey.h"
#include<iostream>

SeriesKey::SeriesKey() {
	name_length_ = 0;
	tag_num_ = 0;
}

SeriesKey::SeriesKey(uint16_t name_length, string name, uint32_t tag_num, vector<Tag> tags) {
	name_length_ = name_length;
	name_ = name;
	tag_num_ = tag_num;
	tags_ = tags;
}

SeriesKey::~SeriesKey() {
}

void SeriesKey::Init() {
	sort(tags_.begin(), tags_.end());
}

void SeriesKey::Init(char data[257]) {
	char tmp_2[3] = {};
	int offset = 256;
	offset -= 2;
	::memcpy(tmp_2, data + offset, 2);
	tag_num_ = atof(tmp_2);
	tags_.resize(tag_num_);
	for (int i = tag_num_-1; i >= 0 ; i--) {
		offset -= 2;
		Tag new_tag;
		tags_[i] = new_tag;
		::memcpy(tmp_2, data + offset, 2);
		tags_[i].tagvalue_length_ = atof(tmp_2);

		char tmp_tagvalue[12] = {};
		offset -= tags_[i].tagvalue_length_;
		::memcpy(tmp_tagvalue, data + offset, tags_[i].tagvalue_length_);
		tags_[i].tagvalue_ = tmp_tagvalue;

		offset -= 2;
		::memcpy(tmp_2, data + offset, 2);
		tags_[i].tagkey_length_ = atof(tmp_2);

		char tmp_tagkey[12]={};
		offset -= tags_[i].tagkey_length_;
		::memcpy(tmp_tagkey, data + offset, tags_[i].tagkey_length_);
		tags_[i].tagkey_ = tmp_tagkey;
	}

	offset -= 2;
	::memcpy(tmp_2, data + offset, 2);
	name_length_ = atof(tmp_2);

	char tmp_name[12] = {};
	offset -= name_length_;
	::memcpy(tmp_name, data + offset, name_length_);
	name_ = tmp_name;
}

int SeriesKey::GetSize() {
	int size = 4 + name_length_;
	for (int i = 0; i < tag_num_; i++) {
		size += 4 + tags_[i].tagkey_length_ + tags_[i].tagvalue_length_;
	}
	return size;
}

bool SeriesKey::operator==(const SeriesKey& series_key) const {
	if (name_ != series_key.name_)return false;
	if (tag_num_ != series_key.tag_num_) return false;
	for (int i = 0; i < tag_num_; i++) {
		if (tags_[i].tagkey_ != series_key.tags_[i].tagkey_)return false;
		if (tags_[i].tagvalue_ != series_key.tags_[i].tagvalue_)return false;
	}
	return true;
}

bool SeriesKey::operator!=(const SeriesKey& series_key) const {
	if (name_ == series_key.name_)return false;
	if (tag_num_ == series_key.tag_num_) return false;
	int i = 0;
	for (; i < tag_num_; i++) {
		if (tags_[i].tagkey_ == series_key.tags_[i].tagkey_)return false;
		if (tags_[i].tagvalue_ == series_key.tags_[i].tagvalue_)return false;
		if (i == series_key.tag_num_ - 1) {
			return false;
		}
	}
	if (series_key.tag_num_ > i)return false;
	return true;
}

bool SeriesKey::operator<(const SeriesKey& series_key) const {
	if (name_ < series_key.name_)return true;
	if (name_ > series_key.name_)return false;

	for (int i = 0; i < tag_num_; i++) {
		if (tags_[i].tagkey_ < series_key.tags_[i].tagkey_)return true;
		if (tags_[i].tagkey_ > series_key.tags_[i].tagkey_)return false;
		if (tags_[i].tagvalue_ < series_key.tags_[i].tagvalue_)return true;
		if (tags_[i].tagvalue_ > series_key.tags_[i].tagvalue_)return false;
	}
	if (tag_num_ < series_key.tag_num_) return true;
	return false;
}

bool SeriesKey::operator>(const SeriesKey& series_key) const {
	if (name_ > series_key.name_)return true;
	if (name_ < series_key.name_)return false;

	for (int i = 0; i < tag_num_; i++) {
		if (tags_[i].tagkey_ > series_key.tags_[i].tagkey_)return true;
		if (tags_[i].tagkey_ < series_key.tags_[i].tagkey_)return false;
		if (tags_[i].tagvalue_ > series_key.tags_[i].tagvalue_)return true;
		if (tags_[i].tagvalue_ < series_key.tags_[i].tagvalue_)return false;
	}
	if (tag_num_ > series_key.tag_num_) return true;
	return false;
}

bool SeriesKey::operator()(const SeriesKey& left, const SeriesKey& right) const {
	return left == right;
}

string SeriesKey::ToString() {
	string result  = name_;
	for (int i = 0; i < tag_num_; i++) {
		result += tags_[i].tagkey_;
		result += tags_[i].tagvalue_;
	}
	return result;
}