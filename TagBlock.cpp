#include"TagBlock.h"

TagBlock::TagBlock() {
	block_trailer_.tagkey_hash_offset_ = 0;
	block_trailer_.tagkey_hash_size_ = 0;
	block_trailer_.tag_key_offset_ = 0;
	block_trailer_.tag_key_size_ = 0;
	block_trailer_.tag_value_offset_ = 0;
	block_trailer_.tag_value_size_ = 0;
}

void TagBlock::InitBlockTrailer(char data[49]) {
	int offset = 0;
	char tmp_8[9] = {};

	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.tag_value_offset_ = atof(tmp_8);

	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.tag_value_size_ = atof(tmp_8);

	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.tag_key_offset_ = atof(tmp_8);

	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.tag_key_size_ = atof(tmp_8);

	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.tagkey_hash_offset_ = atof(tmp_8);

	memcpy(tmp_8, data + offset, 8);
	offset += 8;
	block_trailer_.tagkey_hash_size_ = atof(tmp_8);
}

void TagBlock::UpdateOffset(int change_size) {
	int num = tag_key_hash_.size();
	for (auto iter = tag_key_hash_.begin(); iter != tag_key_hash_.end(); iter++){
		tag_key_hash_[iter->first] = { iter->second.first + change_size ,iter->second.second + change_size };
	}
	for (int i = 0; i < num; i++) {
		tag_keys_[i].tagvalue_hash_offset_ += change_size;
		for (auto iter = tag_values_[i].tag_value_hash_.begin(); iter != tag_values_[i].tag_value_hash_.end(); iter++) {
			tag_values_[i].tag_value_hash_[iter->first] = { iter->second.first + change_size ,iter->second.second};
		}
	}
	block_trailer_.tagkey_hash_offset_ += change_size;
	block_trailer_.tag_key_offset_ += change_size;
	block_trailer_.tag_value_offset_ += change_size;
}

int TagBlock::Insert(string tagkey, string tagvalue, SeriesId seriesid, int* block_size, int* block_offset) {
	int change_size = 0;
	if (tag_key_hash_.empty()) {
		block_trailer_.tagkey_hash_offset_ = *block_offset;
		block_trailer_.tag_key_offset_ = *block_offset;
		block_trailer_.tag_value_offset_ = *block_offset;
	}
	if (tag_key_hash_.count(tagkey) == 0) {
		change_size += 32;
		block_trailer_.tagkey_hash_size_ += 32;
		int index = tag_key_hash_.size();
		int size = 0;
		int offset = *block_offset;
		if (index - 1 >= 0) {
			string pre = tag_keys_[index - 1].key_;
			pair<uint32_t, uint32_t> tmp = tag_key_hash_[pre];
			offset = tmp.first;
			size = tmp.second;
		}

		TagKey new_key;
		new_key.key_ = tagkey;
		new_key.key_length_ = tagkey.length();
		tag_key_hash_[tagkey] = { size + offset, new_key.key_length_ + 18 };
		change_size += new_key.key_length_ + 18;
		block_trailer_.tag_key_size_ += new_key.key_length_ + 18;
		block_trailer_.tagkey_hash_offset_ += new_key.key_length_ + 18;

		tag_values_[index].tag_values_[0].value_ = tagvalue;
		tag_values_[index].tag_values_[0].value_length_ = tagvalue.length();
		tag_values_[index].tag_values_[0].seriesids_.push_back(seriesid);
		tag_values_[index].tag_values_[0].series_num_++;
		int offset_value = *block_offset;
		if (index != 0) {
			int number = tag_values_[index - 1].tag_value_hash_.size();
			string value = tag_values_[index - 1].tag_values_[number - 1].value_;
			offset_value = tag_values_[index - 1].tag_value_hash_[value].first + tag_values_[index - 1].tag_value_hash_[value].second + number * 32;
		}
		tag_values_[index].tag_value_hash_[tagvalue] = { offset_value ,8 + tagvalue.length() };

		new_key.tagvalue_hash_offset_ = offset_value + 8 + tagvalue.length();
		new_key.tagvalue_hash_size_ = 32;
		tag_keys_[index] = new_key;
		for (auto iter = tag_key_hash_.begin(); iter != tag_key_hash_.end(); iter++) {
			iter->second.first+= 32 + 8 + tagvalue.length();
		}

		change_size += 32 + 8 + tagvalue.length();
		block_trailer_.tag_value_size_ += 32 + 8 + tagvalue.length();
		block_trailer_.tag_key_offset_ += 32 + 8 + tagvalue.length();
		block_trailer_.tagkey_hash_offset_ += 32 + 8 + tagvalue.length();

		*block_size = block_trailer_.tagkey_hash_size_ + block_trailer_.tag_key_size_ + block_trailer_.tag_value_size_ + 48;
		return change_size;
	}

	int index_key = 0;
	for (index_key = 0; index_key < tag_key_hash_.size(); index_key++) {
		if (tag_keys_[index_key].key_ == tagkey) break;
	}
	int index_value = 0;
	for (index_value = 0; index_value < tag_values_[index_key].tag_value_hash_.size(); index_value++) {
		if (tag_values_[index_key].tag_values_[index_value].value_ == tagvalue) break;
	}

	int offset_value = 0;
	if (index_value == tag_values_[index_key].tag_value_hash_.size()) {
		tag_values_[index_value].tag_values_[0].value_ = tagvalue;
		tag_values_[index_value].tag_values_[0].value_length_ = tagvalue.length();
		tag_values_[index_value].tag_values_[0].seriesids_.push_back(seriesid);
		tag_values_[index_value].tag_values_[0].series_num_++;
		int number = tag_values_[index_value - 1].tag_value_hash_.size();
		string value = tag_values_[index_value - 1].tag_values_[number - 1].value_;
		offset_value = tag_values_[index_value - 1].tag_value_hash_[value].first + tag_values_[index_value - 1].tag_value_hash_[value].second + number * 32;
		tag_values_[index_value].tag_value_hash_[tagvalue] = { offset_value ,8 + tagvalue.length() };
		for (auto iter = tag_key_hash_.begin(); iter != tag_key_hash_.end(); iter++) {
			iter->second.second += 32 + 8 + tagvalue.length();
		}
		change_size += 32 + 8 + tagvalue.length();
		tag_keys_[index_key].tagvalue_hash_size_ += 32;
		tag_keys_[index_key].tagvalue_hash_offset_ +=  8 + tagvalue.length();
		block_trailer_.tag_value_size_+= 32 + 8 + tagvalue.length();
		block_trailer_.tagkey_hash_offset_+= 32 + 8 + tagvalue.length();
		block_trailer_.tag_key_offset_+= 32 + 8 + tagvalue.length();

		*block_size = block_trailer_.tagkey_hash_size_ + block_trailer_.tag_key_size_ + block_trailer_.tag_value_size_ + 48;
		return change_size;
	}
	else {
		tag_values_[index_key].tag_values_[index_value].seriesids_.push_back(seriesid);
		tag_values_[index_key].tag_values_[index_value].series_num_++;
		change_size += 4;
		tag_values_[index_key].tag_value_hash_[tagvalue].second += 4;
		for (int i = index_value + 1; i < tag_values_[index_key].tag_value_hash_.size(); i++) {
			string value = tag_values_[index_key].tag_values_[i].value_;
			tag_values_[index_key].tag_value_hash_[value].first += 4;
		}
		tag_keys_[index_key].tagvalue_hash_offset_ += 4;

		for (int i =0; i < tag_key_hash_.size(); i++) {
			if (i > index_key) {
				for (int j = 0; j < tag_values_[i].tag_value_hash_.size();j++) {
					string value = tag_values_[i].tag_values_[j].value_;
					tag_values_[i].tag_value_hash_[value].first += 4;
				}
				tag_keys_[index_key].tagvalue_hash_offset_ += 4;
			}
			tag_key_hash_[tag_keys_[i].key_].first += 4;
		}
		block_trailer_.tagkey_hash_offset_ += 4;
		block_trailer_.tag_key_offset_ += 4;
		block_trailer_.tag_value_size_ += 4;

		*block_size = block_trailer_.tagkey_hash_size_ + block_trailer_.tag_key_size_ + block_trailer_.tag_value_size_ + 48;
		return change_size;
	}

	*block_size = block_trailer_.tagkey_hash_size_ + block_trailer_.tag_key_size_ + block_trailer_.tag_value_size_ + 48;
	return change_size;
}
