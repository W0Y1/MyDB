#include"TSMFile.h"

//key1大于key2返回1 key1小于key2返回-1 相等返回0
int CompareKey(const Key& key1,const Key& key2) {
	if (key1.series_key_ > key2.series_key_) return 1;
	if (key1.series_key_ < key2.series_key_) return -1;

	string key1_fieldkey = key1.field_key_;
	string key2_fieldkey = key2.field_key_;
	if (key1_fieldkey > key2_fieldkey) return 1;
	if (key1_fieldkey < key2_fieldkey) return -1;

	return 0;
}

TSMFile::TSMFile() {
	sdbs_.clear();
	sibs_.clear();
	index_offset_ = 0;
	index_size_ = 0;
}

TSMFile::TSMFile(string path) {
	this->path_ = path;
	db_io_.open(path, ios::in | ios::binary);
	if (!db_io_.is_open()) {
		index_offset_ = 0;
		index_size_ = 0;
		return;
	}
	db_io_.seekg(-16, ios::end);
	char buffer[17] = {};
	db_io_.read(buffer, 16);
	char tmp_8[9]={};
	::memcpy(tmp_8, buffer, 8);
	index_offset_=atof(tmp_8);
	::memcpy(tmp_8, buffer+8, 8);
	index_size_ = atof(tmp_8);

	//加载索引部分
	int number = index_size_ / SERIIES_INDEX_BLOCK_SIZE;

	char* buffer_index = new char[index_size_ + 1];
	db_io_.seekg(index_offset_, ios::beg);
	db_io_.read(buffer_index, index_size_);

	for (int i = 0; i < number; i++) {
		char tmp[SERIIES_INDEX_BLOCK_SIZE] = {};
		::memcpy(tmp, buffer_index + i * SERIIES_INDEX_BLOCK_SIZE, SERIIES_INDEX_BLOCK_SIZE);
		SeriesIndexBlock* sib = new SeriesIndexBlock;
		sib->init(tmp);
		sibs_.push_back(sib);
	}
	delete[]buffer_index;
	db_io_.close();
}

TSMFile::~TSMFile() {
	for (int i = 0; i < sdbs_.size(); i++) {
		delete sdbs_[i];
	}
	for (int i = 0; i < sibs_.size(); i++) {
		delete sibs_[i];
	}
}

void TSMFile::SetPath(string path) {
	this->path_ = path;
	db_io_.open(path, ios::in | ios::binary);
	if (!db_io_.is_open()) {
		index_offset_ = 0;
		index_size_ = 0;
		return;
	}
	db_io_.seekg(-16, ios::end);
	char buffer[17] = {};
	db_io_.read(buffer, 16);
	char tmp_8[9] = {};
	::memcpy(tmp_8, buffer, 8);
	index_offset_ = atof(tmp_8);
	::memcpy(tmp_8, buffer + 8, 8);
	index_size_ = atof(tmp_8);

	//加载索引部分
	int number = index_size_ / SERIIES_INDEX_BLOCK_SIZE;

	char* buffer_index = new char[index_size_ + 1];
	db_io_.seekg(index_offset_, ios::beg);
	db_io_.read(buffer_index, index_size_);

	for (int i = 0; i < number; i++) {
		char tmp[SERIIES_INDEX_BLOCK_SIZE + 1] = {};
		::memcpy(tmp, buffer_index + i * SERIIES_INDEX_BLOCK_SIZE, SERIIES_INDEX_BLOCK_SIZE);
		SeriesIndexBlock* sib = new SeriesIndexBlock;
		sib->init(tmp);
		sibs_.push_back(sib);
	}
	delete[]buffer_index;
	db_io_.close();
}

int TSMFile::SearchIndexBlock(const Key& key) {
	int num = index_size_ / SERIIES_INDEX_BLOCK_SIZE;
	if (num == 0)return -100;
	int low = 0;
	int high = num - 1;
	int mid;

	if (CompareKey(sibs_[low]->file_trailer_.key_, key) == 0) return low;
	if (CompareKey(sibs_[high]->file_trailer_.key_, key) == 0) return high;

	if (CompareKey(sibs_[high]->file_trailer_.key_, key) == -1) return 100;
	if (CompareKey(sibs_[low]->file_trailer_.key_, key) == 1)	return -1;

	while (low < high) {
		mid = (low + high) / 2;
		if (CompareKey(sibs_[mid]->file_trailer_.key_, key) == 0) return mid;
		else if(CompareKey(sibs_[mid]->file_trailer_.key_, key) == 1) {
			high = mid;
		}
		else {
			low = mid+1;
		}
	}
	return low;
}

void TSMFile::NewIndexBlock(const Key& key, const char type[8], int index, bool flag) {
	if (index == -100) {
		SeriesIndexBlock* new_block = new SeriesIndexBlock(key, type);
		sibs_.push_back(new_block);
	}
	else {
		sibs_.push_back(nullptr);
		for (int i = index_size_ / SERIIES_INDEX_BLOCK_SIZE; i > index + flag; i--) {
			sibs_[i] = sibs_[i - 1];
			for (int j = 0; j < sibs_[i]->file_trailer_.count_; j++) {
				sibs_[i]->index_entrys_[j].offset_ += SERIIES_INDEX_BLOCK_SIZE;
			}
		}
		SeriesIndexBlock* new_block = new SeriesIndexBlock(key, type);
		sibs_[index + flag] = new_block;
	}
}

bool TSMFile::InsertData(const Key& key, string field_value, uint64_t timestamp, const char type[8]) {
	int index;
	index = SearchIndexBlock(key);
	if (index == 100) {
		int num = index_size_ / SERIIES_INDEX_BLOCK_SIZE - 1;
		int size = 0;
		int offset = 0;
		int index_offset = sibs_[num]->GetCount() - 1;
		IndexEntry* tar_entry = &sibs_[num]->index_entrys_[index_offset];
		size = tar_entry->size_;
		offset = tar_entry->offset_;

		SeriesDataBlock* new_datablock=new SeriesDataBlock(type);
		new_datablock->InsertNewData(timestamp, field_value);
		sdbs_.push_back(new_datablock);

		this->index_offset_ += DATA_BLOCK_MAXSIZE;
		NewIndexBlock(key, type, num, 1);
		this->index_size_ += SERIIES_INDEX_BLOCK_SIZE;
		sibs_[num+1]->AddNewIndexEntry(timestamp, timestamp, 11 + sizeof(timestamp) + sizeof(field_value), offset + DATA_BLOCK_MAXSIZE);

	}
	else if (index == -1) {
		int size = 0;
		int offset = 0;
		IndexEntry* tar_entry = &sibs_[0]->index_entrys_[0];
		size = tar_entry->size_;
		offset = tar_entry->offset_;

		SeriesDataBlock* new_datablock = new SeriesDataBlock(type);
		new_datablock->InsertNewData(timestamp, field_value);
		int count = 0;
		sdbs_.push_back(nullptr);
		for (int i = 0; i < index_size_ / SERIIES_INDEX_BLOCK_SIZE; i++) {
			count += sibs_[i]->GetCount();
		}
		for (int i =count; i > 0; i--) {
			sdbs_[i] = sdbs_[i - 1];
		}
		sdbs_[0] = new_datablock;
		this->index_offset_ += DATA_BLOCK_MAXSIZE;
		NewIndexBlock(key, type, 0, 0);
		this->index_size_ += SERIIES_INDEX_BLOCK_SIZE;
		sibs_[index]->AddNewIndexEntry(timestamp, timestamp, 27 + sizeof(field_value), offset);
	}
	else if (index == -100) {
		int offset = 0;
		SeriesDataBlock* new_datablock = new SeriesDataBlock(type);
		new_datablock->InsertNewData(timestamp, field_value);
		sdbs_.push_back(new_datablock);
		this->index_offset_ += DATA_BLOCK_MAXSIZE;
		NewIndexBlock(key, type, -100, 0);
		sibs_[0]->AddNewIndexEntry(timestamp, timestamp, 27 + sizeof(field_value), offset);
		this->index_size_ += SERIIES_INDEX_BLOCK_SIZE;
	}
	else if (CompareKey(sibs_[index]->file_trailer_.key_, key)==0) {
		int size = 0;
		int offset = 0;
		int index_offset = sibs_[index]->GetCount() - 1;
		IndexEntry *tar_entry = &sibs_[index]->index_entrys_[index_offset];
		size = tar_entry->size_;
		offset = tar_entry->offset_;

		int count = index_offset;
		for (int i = 0; i < index; i++) {
			count += sibs_[i]->GetCount();  
		}
		sdbs_[count]->InsertNewData(timestamp, field_value);
		sibs_[index]->Update(sdbs_[count]->GetMinTime(), sdbs_[count]->GetMaxTime(), 16 + sizeof(field_value), index_offset);
	}
	else if (CompareKey(sibs_[index]->file_trailer_.key_, key) == 1) {		//如果key小于目标key,插入到左侧
		int size = 0;
		int offset = 0;
		IndexEntry* tar_entry = &sibs_[index]->index_entrys_[0];
		size = tar_entry->size_;
		offset = tar_entry->offset_;

		SeriesDataBlock* new_datablock = new SeriesDataBlock(type);
		new_datablock->InsertNewData(timestamp, field_value);

		int count = 0;
		int count_2 = 0;
		sdbs_.push_back(nullptr);
		for (int i = 0; i < index_size_ / SERIIES_INDEX_BLOCK_SIZE; i++) {
			if (i == index) count_2 = count;
			count += sibs_[i]->GetCount();
		}
		for (int i = count; i > count_2; i--) {
			sdbs_[i] = sdbs_[i - 1];
		}
		sdbs_[count_2] = new_datablock;

		this->index_offset_ += DATA_BLOCK_MAXSIZE;
		NewIndexBlock(key, type, index, 0);
		this->index_size_ += SERIIES_INDEX_BLOCK_SIZE;
		sibs_[index]->AddNewIndexEntry(timestamp, timestamp, 27 + sizeof(field_value), offset);
	}
	else {																											//如果key大于目标key,插入到右侧
		int size = 0;
		int offset = 0;
		int index_offset = sibs_[index]->GetCount() - 1;
		IndexEntry* tar_entry = &sibs_[index]->index_entrys_[index_offset];
		size = tar_entry->size_;
		offset = tar_entry->offset_;

		SeriesDataBlock* new_datablock = new SeriesDataBlock(type);
		new_datablock->InsertNewData(timestamp, field_value);

		int count = 0;
		int count_2 = 0;
		for (int i = 0; i < index_size_ / SERIIES_INDEX_BLOCK_SIZE; i++) {
			count += sibs_[i]->GetCount();
			if (i == index) count_2 = count;
		}
		sdbs_.push_back(nullptr);
		for (int i = count; i > count_2; i--) {
			sdbs_[i] = sdbs_[i - 1];
		}
		sdbs_[count_2] = new_datablock;

		this->index_offset_ += DATA_BLOCK_MAXSIZE;
		NewIndexBlock(key, type, index, 1);
		this->index_size_ += SERIIES_INDEX_BLOCK_SIZE;
		sibs_[index]->AddNewIndexEntry(timestamp, timestamp, 11 + sizeof(timestamp) + sizeof(field_value), offset+ DATA_BLOCK_MAXSIZE);
	}
	return true;
}

void TSMFile::FlushToFile()
{
	int size_data = 0;
	int count = 0;
	for (int i = 0; i < index_size_ / SERIIES_INDEX_BLOCK_SIZE; i++) {
		count += sibs_[i]->GetCount();
	}
	size_data = count * DATA_BLOCK_MAXSIZE;

	char* buffer_data = new char[size_data+1];
	int offset_buffer = 0;
	for (int i = 0; i < count; i++) {
		offset_buffer = i * DATA_BLOCK_MAXSIZE;

		::memcpy(buffer_data, sdbs_[i]->type_, 8);
		offset_buffer += 8;
		char tmp_3[4] = {};
		::memcpy(buffer_data + offset_buffer, to_string(sdbs_[i]->length_).c_str(), 3);
		offset_buffer += 3;
		char tmp_16[17] = {};
		for (int j = 0; j < sdbs_[i]->length_; j++) {
			::memcpy(buffer_data + offset_buffer, to_string(sdbs_[i]->timestamps[j]).c_str(), 16);
			offset_buffer += 16;
		}
		string type = sdbs_[i]->type_;
		if (type == "bool") {
			for (int j = 0; j < sdbs_[i]->length_; j++) {
				if (sdbs_[i]->values[j] == "1") {
					::memcpy(buffer_data + offset_buffer, sdbs_[i]->values[j].c_str(), 1);
				}
				else {
					::memcpy(buffer_data + offset_buffer, sdbs_[i]->values[j].c_str(), 1);
				}
				offset_buffer += 1;
			}
		}
		else if (type == "int") {
			for (int j = 0; j < sdbs_[i]->length_; j++) {
				::memcpy(buffer_data + offset_buffer, sdbs_[i]->values[j].c_str(), 12);
				offset_buffer += 12;
			}
		}
		else if (type == "double") {
			for (int j = 0; j < sdbs_[i]->length_; j++) {
				::memcpy(buffer_data + offset_buffer, sdbs_[i]->values[j].c_str(), 16);
				offset_buffer += 16;
			}
		}
		else {
			for (int j = 0; j < sdbs_[i]->length_; j++) {
				::memcpy(buffer_data + offset_buffer, sdbs_[i]->values[j].c_str(), 32);
				offset_buffer += 32;
			}
		}
	}

	char* buffer_index = new char[index_size_ + 16 + 1];
	int offset = 0;
	for (int i = 0; i < index_size_ / SERIIES_INDEX_BLOCK_SIZE; i++) {
		int num = sibs_[i]->file_trailer_.count_;
		offset = i * SERIIES_INDEX_BLOCK_SIZE;
		for (int j = 0; j < num; j++) {
			::memcpy(buffer_index + offset, to_string(sibs_[i]->index_entrys_[j].max_time_).c_str(), 16);
			offset += 16;
			::memcpy(buffer_index + offset, to_string(sibs_[i]->index_entrys_[j].min_time_).c_str(), 16);
			offset += 16;

			::memcpy(buffer_index + offset, to_string(sibs_[i]->index_entrys_[j].offset_).c_str(), 8);
			offset += 8;
			::memcpy(buffer_index + offset, to_string(sibs_[i]->index_entrys_[j].size_).c_str(), 8);
			offset += 8;
		}

		offset = (i + 1) * SERIIES_INDEX_BLOCK_SIZE;
		char tmp_2[3] = {};
		offset -= 2;
		::memcpy(buffer_index + offset, to_string(sibs_[i]->file_trailer_.count_).c_str(), 2);
		offset -= 8;
		::memcpy(buffer_index + offset, sibs_[i]->file_trailer_.type_, 8);
		offset -= 16;
		::memcpy(buffer_index + offset, sibs_[i]->file_trailer_.key_.field_key_, 16);
		
		int name_length = sibs_[i]->file_trailer_.key_.series_key_.name_length_;
		int tag_num = sibs_[i]->file_trailer_.key_.series_key_.tag_num_;

		offset -= 2;
		::memcpy(buffer_index + offset, to_string(tag_num).c_str(), 2);
		

		for (int j = tag_num-1; j >= 0; j--) {
			int tagkey_length = sibs_[i]->file_trailer_.key_.series_key_.tags_[j].tagkey_length_;
			int tagvalue_length = sibs_[i]->file_trailer_.key_.series_key_.tags_[j].tagvalue_length_;

			offset -= 2;
			::memcpy(buffer_index + offset, to_string(tagvalue_length).c_str(), 2);

			offset -= tagvalue_length;
			::memcpy(buffer_index + offset, sibs_[i]->file_trailer_.key_.series_key_.tags_[j].tagvalue_.c_str(), tagvalue_length);

			offset -= 2;
			::memcpy(buffer_index + offset, to_string(tagkey_length).c_str(), 2);

			offset -= tagkey_length;
			::memcpy(buffer_index + offset, sibs_[i]->file_trailer_.key_.series_key_.tags_[j].tagkey_.c_str(), tagkey_length);
		}
		offset -= 2;
		::memcpy(buffer_index + offset, to_string(sibs_[i]->file_trailer_.key_.series_key_.name_length_).c_str(), 2);
		
		offset -= sibs_[i]->file_trailer_.key_.series_key_.name_length_;
		::memcpy(buffer_index + offset, sibs_[i]->file_trailer_.key_.series_key_.name_.c_str(), sibs_[i]->file_trailer_.key_.series_key_.name_length_);
	}

	char tmp_8[9]={};
	offset = index_size_;
	::memcpy(buffer_index + offset, to_string(this->index_offset_).c_str(), 8);
	offset += 8;
	::memcpy(buffer_index + offset, to_string(this->index_size_).c_str(), 8);
	offset += 8;

	db_io_.open(path_, ios::binary | ios::out);
	db_io_.seekg(0, ios::beg);
	db_io_.write(buffer_data, size_data);
	db_io_.seekg(index_offset_, ios::beg);
	db_io_.write(buffer_index, index_size_ + 16);
	db_io_.close();

	delete[]buffer_index;
	delete[]buffer_data;
}

bool TSMFile::SearchData(Key& key, uint64_t timestamp, vector<string> &values) {
	int index = SearchIndexBlock(key);
	if (index == -1 || index == 100) return false;

	int *offset;
	int *size;
	int number = 0;
	vector<pair<int, int>>data_pos;
	sibs_[index]->SearchDataBlock(timestamp, data_pos);

	db_io_.open(path_, ios::in | ios::binary);
	if (!db_io_.is_open())	return false;

	for (int i = 0; i < number; i++) {
		char* data = new char[data_pos[i].second + 1];

		db_io_.seekg(data_pos[i].first, ios::beg);
		db_io_.read(data, data_pos[i].second);
		SeriesDataBlock* sdb = new SeriesDataBlock;
		sdb->init(data, data_pos[i].second);

		string value;
		if (sdb->SearchDataByTime(timestamp,&value)) {
			values.push_back(value);
		}
		delete sdb;
		delete[]data;
	}
	db_io_.close();
	return true;
}

bool TSMFile::SearchData(Key& key, vector<uint64_t>& times, vector<string>& values) {
	int index = SearchIndexBlock(key);
	if (index == -1 || index == 100) return false;

	int* offset;
	int* size;
	vector<pair<int, int>>data_pos;
	sibs_[index]->SearchDataBlock(data_pos);


	db_io_.open(path_, ios::in | ios::binary);
	if (!db_io_.is_open())	return false;

	for (int i = 0; i < data_pos.size(); i++) {
		char* data = new char[data_pos[i].second + 1];

		db_io_.seekg(data_pos[i].first, ios::beg);
		db_io_.read(data, data_pos[i].second);
		SeriesDataBlock* sdb = new SeriesDataBlock;
		sdb->init(data, data_pos[i].second);
		sdb->GetData(times, values);
		
		delete[]data;
		delete sdb;
	}
	db_io_.close();
	return true;
}
