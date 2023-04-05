//#include "BitList(´æÔÚbug).h"
//
//const unsigned char bits[8] =
//{
//		0x01,
//		0x02,
//		0x04,
//		0x08,
//		0x10,
//		0x20,
//		0x40,
//		0x80
//};
//
//BitList::BitList(uint64_t size)
//	:size_(size), block_size_(64)
//{
//	uint64_t tmp = (uint64_t)(size_ / 8 / block_size_);
//
//	while (tmp > 0){
//		unsigned char* data = (unsigned char*)malloc(block_size_);
//		memset(data, 0, block_size_);
//		data_.push_back(data);
//
//		tmp--;
//	}
//
//	unsigned char* data = nullptr;
//	data = (unsigned char*)malloc(size_ % (block_size_ * 8));
//	::memset(data, 0, size_ % (block_size_ * 8));
//	if (data) {
//		data_.push_back(data);
//	}
//}
//
//BitList::~BitList(){
//	for (int i = 0; i < data_.size() / 8; ++i){
//		free(data_[i]);
//	}
//}
//
//void BitList::Init(unsigned char* data) {
//	int offset = 0;
//	uint64_t tmp = (uint64_t)(size_ / 8 / block_size_);
//	for (int i = 0; i < tmp; i++) {		
//		unsigned char* bit_data = (unsigned char*)malloc(block_size_);
//		memset(bit_data, 0, block_size_);
//		memcpy((char*)bit_data, (char*)data + offset, block_size_);
//		data_[i]=bit_data;
//		offset += block_size_;
//	}
//	unsigned char* bit_data = nullptr;
//	bit_data = (unsigned char*)malloc(size_ % (block_size_ * 8));
//	memcpy((char*)bit_data, (char*)data + offset, size_ % (block_size_ * 8));
//	data_[tmp] = bit_data;
//}
//
//long long BitList::size(){
//	return size_;
//}
//
//void BitList::reset(const bool val){
//	for (int i = 0; i < data_.size(); ++i){
//		if (val){
//			memset(data_[i], 0xFF, block_size_);
//		}
//		else{
//			memset(data_[i], 0x00, block_size_);
//		}
//	}
//}
//
//void BitList::clear(){
//	for (int i = 0; i < data_.size(); ++i){
//		free(data_[i]);
//	}
//	data_.clear();
//}
//
//bool BitList::operator[](uint64_t index){
//	return data_[index / (block_size_ * 8)][(index - ((index / (block_size_ * 8)) * 64)) / 8] & bits[index % 8];
//}
//
//void BitList::set_value(uint64_t index, const bool val){
//	if (val) {
//		data_[index / (block_size_ * 8)][(index - ((index / (block_size_ * 8)) * 64)) / 8] |= bits[index % 8];
//	}
//	else {
//		data_[index / (block_size_ * 8)][(index - ((index / (block_size_ * 8)) * 64)) / 8] &= (~(bits[index % 8]));
//	}
//}