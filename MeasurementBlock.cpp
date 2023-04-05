#include"MeasurementBlock.h"

MeasurementBlock::MeasurementBlock() {
	block_trailer_.data_offset_ = 0;
	block_trailer_.data_size_ = 0;
	block_trailer_.hash_offset_ = 0;
	block_trailer_.hash_size_ = 0;
}

void MeasurementBlock::Init(char* data, int size){
	char tmp_8[9] = {};
	memcpy(tmp_8, data + size - 32, 8);
	block_trailer_.data_offset_ = atof(tmp_8);
	memcpy(tmp_8, data + size - 24, 8);
	block_trailer_.data_size_ = atof(tmp_8);
	memcpy(tmp_8, data + size - 16, 8);
	block_trailer_.hash_offset_ = atof(tmp_8);
	memcpy(tmp_8, data + size - 8, 8);
	block_trailer_.hash_size_ = atof(tmp_8);

	char tmp_16[17] = {};
	int offset = 0;
	for (int i = 0; i < block_trailer_.data_size_ / 32; i++) {
		memcpy(tmp_16, data + offset, 16);
		offset += 16;
		measurements_[i].name_ = tmp_16;

		memcpy(tmp_8, data + offset, 8);
		offset += 8;
		measurements_[i].tag_block_offset_ = atof(tmp_8);

		memcpy(tmp_8, data + offset, 8);
		offset += 8;
		measurements_[i].tag_block_size_ = atof(tmp_8);
	}

	char tmp_2[3] = {};
	for (int i = 0; i < block_trailer_.hash_size_ / 10; i++) {
		memcpy(tmp_8, data + offset, 8);
		offset += 8;
		memcpy(tmp_2, data + offset, 2);
		offset += 2;

		hash_index_[tmp_8] = atof(tmp_2);
	}
}

bool MeasurementBlock::SearchTagBlock(string name, uint32_t* tag_block_offset, uint32_t* tag_block_size) {
	if (hash_index_.count(name) == 0) {
		return false;
	}
	int pos = hash_index_[name];
	*tag_block_offset = measurements_[pos].tag_block_offset_;
	*tag_block_size = measurements_[pos].tag_block_size_;
	return true;
}

int MeasurementBlock::UpdateOffset(int change_size) {
	int size = hash_index_.size();
	block_trailer_.hash_offset_ += change_size;
	block_trailer_.data_offset_ += change_size;
	for (int i = 0; i < size; i++) {
		measurements_[i].tag_block_offset_ += change_size;
	}
	return size;
}

int MeasurementBlock::Insert(string name, int* tagblock_offset, int* flag){
	if (hash_index_.count(name) == 0) {
		*flag = 1;
		int index = hash_index_.size();
		hash_index_[name] = index;
		measurements_[index].name_ = name;
		if (index != 0) {
			measurements_[index].tag_block_offset_ = measurements_[index - 1].tag_block_offset_ + measurements_[index - 1].tag_block_size_;
			*tagblock_offset = measurements_[index].tag_block_offset_;
		}
		else {
			measurements_[index].tag_block_offset_ = *tagblock_offset;
		}
		block_trailer_.data_size_ += 32;
		block_trailer_.hash_offset_ += 32;
		block_trailer_.hash_size_ += 10;
		return index;
	}
	*flag = 0;
	return hash_index_[name];
}

void MeasurementBlock::UpdateBlockTrailer(int change_size) {
	block_trailer_.data_offset_ += change_size;
	block_trailer_.hash_offset_ += change_size;
}

void MeasurementBlock::UpdateMeasurement(int tar_index, uint32_t tag_block_offset, uint32_t tag_block_size) {
	measurements_[tar_index].tag_block_offset_ = tag_block_offset;
	measurements_[tar_index].tag_block_size_ = tag_block_size;
}