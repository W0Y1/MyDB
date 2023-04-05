#include "SeriesDataBlock.h"
#include<fstream>

SeriesDataBlock::SeriesDataBlock() {
	length_ = 0;
	timestamps.reserve(8192);
	values.reserve(8192);
}

SeriesDataBlock::SeriesDataBlock(string type) {
	memcpy(type_, type.c_str(), 8);
	length_ = 0;
	timestamps.reserve(8192);
	values.reserve(8192);
}

void SeriesDataBlock::init(char* data, int size) {
	memcpy(type_, data, 8);

	char tmp_3[4]={};
	memcpy(tmp_3, data + 8, 3);
	this->length_ = atof(tmp_3);

	int offset = 11;
	char tmp_16[17] = {};
	for (int i = 0; i < length_; i++) {
		memcpy(tmp_16, data + offset, 16);
		offset += 16;
		timestamps.push_back(atof(tmp_16));
	}
	string type = type_;
	if (type == "bool") {
		for (int i = 0; i < length_; i++) {
			char tmp_1[2] = {};
			memcpy(tmp_1, data + offset, 1);
			values.push_back(tmp_1);
			offset += 1;
		}
	}
	else if (type == "int") {
		char tmp_12[13] = {};
		for (int i = 0; i < length_; i++) {
			memcpy(tmp_12, data + offset, 12);
			values.push_back(tmp_12);
			offset += 12;
		}
	}
	else if (type == "double") {
		char tmp_16[17] = {};
		for (int i = 0; i < length_; i++) {
			memcpy(tmp_16, data + offset, 16);
			values.push_back(tmp_16);
			offset += 16;
		}
	}
	else {
		char tmp_32[33] = {};
		for (int i = 0; i < length_; i++) {
			memcpy(tmp_32, data + offset, 32);
			values.push_back(tmp_32);
			offset += 32;
		}
	}
}

int SeriesDataBlock::BinarySearch(uint64_t timestamp) {
	int low = 0;
	int high = length_ - 1;
	if (high == -1) { return 0; }
	int mid = (low + high) / 2;
	if (timestamp < timestamps[low]) return low;
	if (timestamp > timestamps[high]) return high;

	while (low < high) {
		mid = (low + high) / 2;
		if (timestamps[mid] == timestamp) return mid;
		
		if (timestamps[mid] < timestamp) low = mid + 1;	
		else high = mid;
	}
	return low;
}


bool SeriesDataBlock::InsertNewData(uint64_t timestamp, string value) {
	int index = BinarySearch(timestamp);
	if (length_ == 0) {
		timestamps.push_back(timestamp);
		values.push_back(value);
		this->length_++;
		return true;
	}
	if (timestamps[index] == timestamp) return false;
	if (timestamps[index] < timestamp) {
		timestamps.push_back(timestamps[length_ - 1]);
		values.push_back(values[length_ - 1]);
		for (int i = length_; i--; i > index+1) {
			timestamps[i] = timestamps[i - 1];
			values[i] = values[i - 1];
		}
		timestamps[index + 1] = timestamp;
		values[index + 1] = value;
		this->length_++;
		return true;
	}
	else {
		timestamps.push_back(timestamps[length_ - 1]);
		values.push_back(values[length_ - 1]);
		for (int i = length_; i--; i > index) {
			timestamps[i] = timestamps[i - 1];
			values[i] = values[i - 1];
		}
		timestamps[index] = timestamp;
		values[index] = value;
		this->length_++;
		return true;
	}

}

bool SeriesDataBlock::SearchDataByTime(uint64_t timestamp, string* value) {
	int index = BinarySearch(timestamp);
	if(timestamp!=timestamps[index]) return false;

	*value = values[index];
	return true;
}

bool SeriesDataBlock::GetData(vector<uint64_t>& timestamp, vector<string>& value) {
	timestamp.insert(timestamp.end(), timestamps.begin(), timestamps.end());
	value.insert(value.end(), values.begin(), values.end());
	if (timestamp.size() == 0)return false;
	return true;
}