#include"TSIFile.h"

TSIFile::TSIFile() {
	file_trailer_.id_ = -1;
	file_trailer_.mbk_offset_ = 36;
	file_trailer_.mbk_size_ = 32;
	file_trailer_.sbk_offset_ = 0;
	file_trailer_.sbk_size_ = 36;
	file_trailer_.tbks_offset_ = 36;
	file_trailer_.tbks_size_ = 0;
	sbk_ = new SeriesBlock;
	mbk_ = new MeasurementBlock;
}

TSIFile::TSIFile(string path, int id) {
	path_ = path;
	file_trailer_.id_ = id;
	file_trailer_.mbk_offset_ = 0;
	file_trailer_.mbk_size_ = 0;
	file_trailer_.sbk_offset_ = 0;
	file_trailer_.sbk_size_ = 0;
	file_trailer_.tbks_offset_ = 0;
	file_trailer_.tbks_size_ = 0;

	db_io_.open(path_, ios::in | ios::binary);

	db_io_.seekg(- 48, ios::end);
	char data[49] = {};
	db_io_.read(data, 48);
	db_io_.close();

	char tmp_8[9] = {};
	::memcpy(tmp_8, data, 8);
	this->file_trailer_.sbk_offset_ = atof(tmp_8);
	::memcpy(tmp_8, data + 8, 8);
	this->file_trailer_.sbk_size_ = atof(tmp_8);
	::memcpy(tmp_8, data + 16, 8);
	this->file_trailer_.tbks_offset_ = atof(tmp_8);
	::memcpy(tmp_8, data + 24, 8);
	this->file_trailer_.tbks_size_ = atof(tmp_8);
	::memcpy(tmp_8, data + 32, 8);
	this->file_trailer_.mbk_offset_ = atof(tmp_8);
	::memcpy(tmp_8, data + 40, 8);
	this->file_trailer_.mbk_size_ = atof(tmp_8);


	sbk_ = new SeriesBlock;
	mbk_ = new MeasurementBlock;
}

TSIFile::~TSIFile() {
	delete sbk_;
	delete mbk_;
}

void TSIFile::Init() {
	lock_guard<mutex> locker(latch_);

	db_io_.open(path_, ios::in | ios::binary);
	if (!db_io_.is_open()) return;

	db_io_.seekg(-48, ios::end);
	char data[49] = {};
	db_io_.read(data, 48);
	db_io_.close();

	char tmp_8[9] = {};
	::memcpy(tmp_8, data, 8);
	this->file_trailer_.sbk_offset_ = atof(tmp_8);
	::memcpy(tmp_8, data + 8, 8);
	this->file_trailer_.sbk_size_ = atof(tmp_8);
	::memcpy(tmp_8, data + 16, 8);
	this->file_trailer_.tbks_offset_ = atof(tmp_8);
	::memcpy(tmp_8, data + 24, 8);
	this->file_trailer_.tbks_size_ = atof(tmp_8);
	::memcpy(tmp_8, data + 32, 8);
	this->file_trailer_.mbk_offset_ = atof(tmp_8);
	::memcpy(tmp_8, data + 40, 8);
	this->file_trailer_.mbk_size_ = atof(tmp_8);
}

bool TSIFile::SearchSeriesKey(SeriesKey serieskey) {
	lock_guard<mutex> locker(latch_);

	db_io_.open(path_, ios::in | ios::binary);
	if (!db_io_.is_open()) return false;

    db_io_.seekg(file_trailer_.sbk_offset_ + file_trailer_.sbk_size_ - 36,ios::beg);
	char data[37] = {};
	db_io_.read(data, 36);
	sbk_->InitBlockTrailer(data);

	db_io_.seekg(sbk_->block_trailer_.bloom_filter_offset, ios::beg);
	vector<int>data_filter;
	for (int i = 0; i < BLOOM_FILTER_SIZE / 32 + 1; i++) {
		char tmp_12[13] = {};
		db_io_.read(tmp_12, 12);
		data_filter.push_back(atof(tmp_12));
	}
	sbk_->InitBloomFilter(data_filter);
	
	if (!sbk_->SearchByFilter(serieskey)) {
		db_io_.close();
		return false;
	}
	db_io_.seekg(sbk_->block_trailer_.series_index_offset,  ios::beg);

	char* data_index = new char[sbk_->block_trailer_.series_index_num * ENTRY_SIZE + 1];
	db_io_.read(data_index, sbk_->block_trailer_.series_index_num * ENTRY_SIZE);

	sbk_->InitIndex(data_index);
	db_io_.close();

	delete[]data_index;
	return sbk_->SearchByIndex(serieskey);

}

vector<SeriesId> TSIFile::FindSeriesId(string name, string tag_key, string tag_value) {
	lock_guard<mutex> locker(latch_);

 	uint32_t tag_block_offset = 0;
	uint32_t tag_block_size = 0;
	db_io_.open(path_, ios::in | ios::binary);
	vector<SeriesId>result;
	if (!db_io_.is_open()) return result;
	db_io_.seekg(file_trailer_.mbk_offset_,ios::beg);
	char* tmp = new char[file_trailer_.mbk_size_ + 1];
	db_io_.read(tmp, file_trailer_.mbk_size_);
	this->mbk_->Init(tmp, file_trailer_.mbk_size_);

	if (!this->mbk_->SearchTagBlock(name, &tag_block_offset, &tag_block_size)) return result;

	db_io_.seekg(tag_block_offset + tag_block_size - 48, ios::beg);
	char data[49]={};
	db_io_.read(data, 48);

	this->tbks_[0].InitBlockTrailer(data);
	int offset_keyhash = tbks_[0].GetTagKeyHashOffset();
	db_io_.seekg(tbks_[0].GetTagKeyHashOffset(),ios::beg);

	int size = tbks_[0].GetTagKeyHashSize();
	char* data_hash = new char[size + 1];
	db_io_.read(data_hash, size);

	uint32_t tagkey_offset = 0;
	uint32_t tagkey_size = 0;
	int offset = 0;
	char first_8[9] = {};
	char second_8[9] = {};
	char tmp_16[17] = {};
	for (int i = 0; i < size / 32; i++) {
		::memcpy(tmp_16, data_hash + offset, 16);
		if (tmp_16 != tag_key) {
			offset += 32;
			continue;
		}
		offset += 16;
		::memcpy(first_8, data_hash + offset, 8);
		offset += 8;
		::memcpy(second_8, data_hash + offset, 8);

		break;
	}
	tagkey_offset = atof(first_8);
	tagkey_size = atof(second_8);

	delete[]data_hash;
	char* data_tagkey = new char[tagkey_size + 1];

	db_io_.seekg(tagkey_offset);
	db_io_.read(data_tagkey, tagkey_size);

	int tagvalue_hash_offset;
	int tagvalue_hash_size;

	char tmp_8[9] = {};
	::memcpy(tmp_8, data_tagkey + (tagkey_size - 16), 8);
	tagvalue_hash_offset = atof(tmp_8);
	::memcpy(tmp_8, data_tagkey + (tagkey_size - 8), 8);
	tagvalue_hash_size = atof(tmp_8);

	delete[]data_tagkey;
	char* hash = new char[tagvalue_hash_size+1];
	db_io_.seekg(tagvalue_hash_offset);
	db_io_.read(hash, tagvalue_hash_size);

	offset = 0;
	for (int i = 0; i < tagvalue_hash_size / 32; i++) {
		::memcpy(tmp_16, hash + offset, 16);
		if (tmp_16 != tag_value) {
			offset += 32;
			continue;
		}
		offset += 16;
		::memcpy(first_8, hash + offset, 8);
		offset += 8;
		::memcpy(second_8, hash + offset, 8);
		
		break;
	}
	int tagvalue_offset = atof(first_8);
	int tagvalue_size = atof(second_8);

	delete[]hash;
	char* tagvalue_data = new char[tagvalue_size+1];
	db_io_.seekg(tagvalue_offset);
	db_io_.read(tagvalue_data, tagvalue_size);

	TagValue tagvalue;
	tagvalue.Init(tagvalue_data);

	delete []tagvalue_data;
	return tagvalue.seriesids_;
}

bool TSIFile::AddNewSeriesKey(SeriesKey serieskey, SeriesId seriesid) {
	lock_guard<mutex> locker(latch_);
	int change_size = 0;
	if(!sbk_->AddSeriesKey(serieskey,&change_size)) return false;
	file_trailer_.sbk_size_ += change_size;
	file_trailer_.mbk_offset_ += change_size;
	file_trailer_.tbks_offset_ += change_size;

	int size = mbk_->UpdateOffset(change_size);
	for (int i = 0; i < size; i++) {
		tbks_[i].UpdateOffset(change_size);
	}

	int tagblock_offset = file_trailer_.tbks_offset_;
	int tagblock_size = 0;
	int flag = 0;
	int pos_tagblock = mbk_->Insert(serieskey.GetName(), &tagblock_offset, &flag);

	if (flag == 1) {
		file_trailer_.mbk_size_ += 42;
		file_trailer_.tbks_size_ += 48;
		file_trailer_.mbk_offset_ += 48;
	}

	change_size = 0;
	for (int i = 0; i < serieskey.GetNum(); i++) {
		Tag tar_tag = serieskey.GetTag(i);
		change_size += tbks_[pos_tagblock].Insert(tar_tag.tagkey_, tar_tag.tagvalue_, seriesid, &tagblock_size, &tagblock_offset);
	}
	file_trailer_.tbks_size_ += change_size;
	file_trailer_.mbk_offset_ += change_size;
	mbk_->UpdateBlockTrailer(change_size);
	mbk_->UpdateMeasurement(pos_tagblock, tagblock_offset, tagblock_size);

	return true;
}

bool TSIFile::FlushToFile() {
	lock_guard<mutex> locker(latch_);

	db_io_.open(path_, ios::out | ios::binary);
	if (!db_io_.is_open()) return false;

	int offset_series = 0;
	char* data_series = new char[file_trailer_.sbk_size_ + 1];
	::memset(data_series, 0, file_trailer_.sbk_size_ + 1);

	for (int i = 0; i < sbk_->block_trailer_.series_index_num; i++) {
		int offset = 0;
		char buffer_index[ENTRY_SIZE + 1] = {};

		if (sbk_->seriesindex_entrys_[i].IsLeafEntry()) {
			SeriesIndex* entry = &sbk_->seriesindex_entrys_[i];
			auto leaf_entry = reinterpret_cast<BPlusTreeLeafEntry*>(entry);

			::memcpy(buffer_index + offset, "1", 1);
			offset += 1;

			::memcpy(buffer_index + offset, to_string(leaf_entry->GetSize()).c_str(), 2);
			offset += 2;

			::memcpy(buffer_index + offset, to_string(leaf_entry->GetParentEntryId()).c_str(), 3);
			offset += 3;

			::memcpy(buffer_index + offset, to_string(leaf_entry->GetNextEntryId()).c_str(), 3);
			offset += 3;
				
			for (int i = 0; i < leaf_entry->GetSize(); i++) {
				char tmp_256[257] = {};
				int offset_key = 256;
				SeriesKey serieskey = leaf_entry->KeyAt(i);
				offset_key -= 2;
				::memcpy(tmp_256 + offset_key, to_string(serieskey.tag_num_).c_str(), 2);

				for (int j = serieskey.tag_num_-1; j >=0; j--) {
					offset_key -= 2;
					::memcpy(tmp_256 + offset_key, to_string(serieskey.tags_[j].tagvalue_length_).c_str(), 2);

					offset_key -= serieskey.tags_[j].tagvalue_length_;
					::memcpy(tmp_256 + offset_key, serieskey.tags_[j].tagvalue_.c_str(), serieskey.tags_[j].tagvalue_length_);

					offset_key -= 2;
					::memcpy(tmp_256 + offset_key, to_string(serieskey.tags_[j].tagkey_length_).c_str(), 2);

					offset_key -= serieskey.tags_[j].tagkey_length_;
					::memcpy(tmp_256 + offset_key, serieskey.tags_[j].tagkey_.c_str(), serieskey.tags_[j].tagkey_length_);
				}

				offset_key -= 2;
				::memcpy(tmp_256 + offset_key, to_string(serieskey.name_length_).c_str(), 2);

				offset_key -= serieskey.name_length_;
				::memcpy(tmp_256 + offset_key, serieskey.name_.c_str(), serieskey.name_length_);

				::memcpy(buffer_index + offset, tmp_256, 256);
				offset += 256;
			}
			::memcpy(data_series+i* ENTRY_SIZE, buffer_index, ENTRY_SIZE);
		}
		else {
			SeriesIndex* entry = &sbk_->seriesindex_entrys_[i];
			auto internal_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
			char tmp_1[2] = "1";
			char tmp_2[3] = {};
			char tmp_3[4] = {};

			::memcpy(buffer_index + offset, tmp_1, 1);
			offset += 1;

			::memcpy(tmp_2, to_string(internal_entry->GetSize()).c_str(), 2);
			::memcpy(buffer_index + offset, tmp_2, 2);
			offset += 2;

			::memcpy(tmp_3, to_string(internal_entry->GetParentEntryId()).c_str(), 3);
			::memcpy(buffer_index + offset, tmp_3, 3);
			offset += 3;

			for (int i = 0; i < internal_entry->GetSize(); i++) {
				char tmp_256[257] = {};
				int offset_key = 256;
				SeriesKey serieskey = internal_entry->KeyAt(i);
				::memcpy(tmp_2, to_string(serieskey.tag_num_).c_str(), 2);
				offset_key -= 2;
				::memcpy(tmp_256 + offset_key, tmp_2, 2);

				for (int j = serieskey.tag_num_ - 1; j >= 0; j--) {
					::memcpy(tmp_2, to_string(serieskey.tags_[j].tagvalue_length_).c_str(), 2);
					offset_key -= 2;
					::memcpy(tmp_256 + offset_key, tmp_2, 2);

					offset_key -= serieskey.tags_[j].tagvalue_length_;
					::memcpy(tmp_256 + offset_key, serieskey.tags_[j].tagvalue_.c_str(), serieskey.tags_[j].tagvalue_length_);

					::memcpy(tmp_2, to_string(serieskey.tags_[j].tagkey_length_).c_str(), 2);
					offset_key -= 2;
					::memcpy(tmp_256 + offset_key, tmp_2, 2);

					offset_key -= serieskey.tags_[j].tagkey_length_;
					::memcpy(tmp_256 + offset_key, serieskey.tags_[j].tagkey_.c_str(), serieskey.tags_[j].tagkey_length_);
				}

				::memcpy(tmp_2, to_string(serieskey.name_length_).c_str(), 2);
				offset_key -= 2;
				::memcpy(tmp_256 + offset_key, tmp_2, 10);

				offset_key -= serieskey.name_length_;
				::memcpy(tmp_256 + offset_key, serieskey.name_.c_str(), serieskey.name_length_);

				::memcpy(buffer_index + offset, tmp_256, 256);
				offset += 256;

				entry_id_t entry_id = internal_entry->ValueAt(i);
				::memcpy(tmp_3, to_string(entry_id).c_str(), 3);
				offset += 3;
				::memcpy(buffer_index + offset, tmp_3, 3);
			}
			::memcpy(data_series + i * ENTRY_SIZE, buffer_index, ENTRY_SIZE);

		}
	}
	offset_series = sbk_->block_trailer_.series_index_num * ENTRY_SIZE;

	char* data_filter = new char[sbk_->block_trailer_.bloom_filter_size + 1];
	::memset(data_filter, 0, sbk_->block_trailer_.bloom_filter_size + 1);

	vector<int> int_data = sbk_->bloom_filter_->ReturnData();
	int offset_filter = 0;
	for (int i = 0; i < int_data.size(); i++) {
		::memcpy(data_filter + offset_filter, to_string(int_data[i]).c_str(), 12);
		offset_filter += 12;
	}

	::memcpy(data_series+ offset_series, data_filter, sbk_->block_trailer_.bloom_filter_size);
	offset_series += sbk_->block_trailer_.bloom_filter_size;

	::memcpy(data_series + offset_series, to_string(sbk_->block_trailer_.index_entry_root_id).c_str(), 4);
	offset_series += 4;

	::memcpy(data_series + offset_series, to_string(sbk_->block_trailer_.series_index_offset).c_str(), 8);
	offset_series += 8;

	::memcpy(data_series + offset_series, to_string(sbk_->block_trailer_.series_index_num).c_str(), 8);
	offset_series += 8;

	::memcpy(data_series + offset_series, to_string(sbk_->block_trailer_.bloom_filter_offset).c_str(), 8);
	offset_series += 8;

	::memcpy(data_series + offset_series, to_string(sbk_->block_trailer_.bloom_filter_size).c_str(), 8);
	offset_series += 8;


	db_io_.seekg(file_trailer_.sbk_offset_,ios::beg);
	db_io_.write(data_series, file_trailer_.sbk_size_);
	delete[]data_series;
	delete[]data_filter;

	char* data_tag = new char[file_trailer_.tbks_size_ + 1];
	::memset(data_tag, 0, file_trailer_.tbks_size_ + 1);
	int offset_tag = 0;
	for (int i = 0; i < mbk_->hash_index_.size(); i++) {
		int num = tbks_[i].tag_key_hash_.size();
		for (int j = 0; j < num; j++) {
			TagValues* cur = &tbks_[i].tag_values_[j];
			int num_value = cur->tag_value_hash_.size();
			for (int m = 0; m < num_value; m++) {
				::memcpy(data_tag + offset_tag, to_string(cur->tag_values_[m].value_length_).c_str(), 2);
				offset_tag += 2;

				::memcpy(data_tag + offset_tag, cur->tag_values_[m].value_.c_str(), cur->tag_values_[m].value_length_);
				offset_tag += cur->tag_values_[m].value_length_;

				::memcpy(data_tag + offset_tag, to_string(cur->tag_values_[m].series_num_).c_str(), 2);
				offset_tag += 2;

				for (int n = 0; n < cur->tag_values_[m].series_num_; n++) {
					::memcpy(data_tag + offset_tag, to_string(cur->tag_values_[m].seriesids_[n]).c_str(), 4);
					offset_tag += 4;
				}
			}

			for (auto iter = cur->tag_value_hash_.begin(); iter != cur->tag_value_hash_.end(); iter++) {
				::memcpy(data_tag + offset_tag, iter->first.c_str(), 16);
				offset_tag += 16;

				::memcpy(data_tag + offset_tag, to_string(iter->second.first).c_str(), 8);
				offset_tag += 8;

				::memcpy(data_tag + offset_tag, to_string(iter->second.second).c_str(), 8);
				offset_tag += 8;
			}
		}

		for (int j = 0; j < num; j++) {
			auto cur = &tbks_[i].tag_keys_[j];

			::memcpy(data_tag + offset_tag, to_string(cur->key_length_).c_str(), 2);
			offset_tag += 2;

			::memcpy(data_tag + offset_tag, cur->key_.c_str(), cur->key_length_);
			offset_tag += cur->key_length_;

			::memcpy(data_tag + offset_tag, to_string(cur->tagvalue_hash_offset_).c_str(), 8);
			offset_tag += 8;

			::memcpy(data_tag + offset_tag, to_string(cur->tagvalue_hash_size_).c_str(), 8);
			offset_tag += 8;
		}

		for (auto iter = tbks_[i].tag_key_hash_.begin(); iter != tbks_[i].tag_key_hash_.end(); iter++) {
			::memcpy(data_tag + offset_tag, iter->first.c_str(), 16);
			offset_tag += 16;

			::memcpy(data_tag + offset_tag, to_string(iter->second.first).c_str(), 8);
			offset_tag += 8;

			::memcpy(data_tag + offset_tag, to_string(iter->second.second).c_str(), 8);
			offset_tag += 8;
		}

		::memcpy(data_tag + offset_tag, to_string(tbks_[i].block_trailer_.tag_value_offset_).c_str(), 8);
		offset_tag += 8;

		::memcpy(data_tag + offset_tag, to_string(tbks_[i].block_trailer_.tag_value_size_).c_str(), 8);
		offset_tag += 8;

		::memcpy(data_tag + offset_tag, to_string(tbks_[i].block_trailer_.tag_key_offset_).c_str(), 8);
		offset_tag += 8;

		::memcpy(data_tag + offset_tag, to_string(tbks_[i].block_trailer_.tag_key_size_).c_str(), 8);
		offset_tag += 8;

		::memcpy(data_tag + offset_tag, to_string(tbks_[i].block_trailer_.tagkey_hash_offset_).c_str(), 8);
		offset_tag += 8;

		::memcpy(data_tag + offset_tag, to_string(tbks_[i].block_trailer_.tagkey_hash_size_).c_str(), 8);
		offset_tag += 8;
	}
	db_io_.seekg(file_trailer_.tbks_offset_,ios::beg);
	db_io_.write(data_tag, file_trailer_.tbks_size_);
	delete[]data_tag;

	char* data_measure = new char[file_trailer_.mbk_size_ + 51];
	::memset(data_measure, 0, file_trailer_.mbk_size_ + 51);
	int offset_measure = 0;
	for (int i = 0; i < mbk_->hash_index_.size(); i++) {
		::memcpy(data_measure + offset_measure, mbk_->measurements_[i].name_.c_str(), 16);
		offset_measure += 16;

		::memcpy(data_measure + offset_measure, to_string(mbk_->measurements_[i].tag_block_offset_).c_str(), 8);
		offset_measure += 8;

		::memcpy(data_measure + offset_measure, to_string(mbk_->measurements_[i].tag_block_size_).c_str(), 8);
		offset_measure += 8;
	}

	for (auto iter = mbk_->hash_index_.begin(); iter != mbk_->hash_index_.end(); iter++) {
		::memcpy(data_measure + offset_measure, iter->first.c_str(), 8);
		offset_measure += 8;

		::memcpy(data_measure + offset_measure, to_string(iter->second).c_str(), 2);
		offset_measure += 2;
	}

	::memcpy(data_measure + offset_measure, to_string(mbk_->block_trailer_.data_offset_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(mbk_->block_trailer_.data_size_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(mbk_->block_trailer_.hash_offset_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(mbk_->block_trailer_.hash_size_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.id_).c_str(), 2);
	offset_measure += 2;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.sbk_offset_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.sbk_size_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.tbks_offset_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.tbks_size_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.mbk_offset_).c_str(), 8);
	offset_measure += 8;

	::memcpy(data_measure + offset_measure, to_string(file_trailer_.mbk_size_).c_str(), 8);
	offset_measure += 8;
	
	db_io_.seekg(file_trailer_.mbk_offset_,ios::beg);
	db_io_.write(data_measure, file_trailer_.mbk_size_ + 50);
	db_io_.close();

	delete[]data_measure;

	return true;
}