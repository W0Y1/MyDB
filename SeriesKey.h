#pragma once
#include<algorithm>
#include"TagBlock.h"
#include<string>

using namespace std;


struct Tag {
	string tagkey_;
	uint16_t tagkey_length_;												//char[2]
	string tagvalue_;
	uint16_t tagvalue_length_;											//char[2]

	Tag(uint16_t tagkey_length, string tagkey, uint16_t tagvalue_length, string tagvalue) {
		tagkey_length_ = tagkey_length;
		tagkey_ = tagkey;
		tagvalue_length_ = tagvalue_length;
		tagvalue_ = tagvalue;
	}

	bool operator<(const Tag& tag) const{
		if (tagkey_ == tag.tagkey_) return tagvalue_ < tag.tagvalue_;
		return tagkey_ < tag.tagkey_;
	}

	bool operator()(const Tag& left,const Tag& right) const {
		if (left.tagkey_ == right.tagkey_) {
			return left.tagvalue_ == right.tagvalue_;
		}
		return left.tagkey_ == right.tagkey_;
	}

	Tag() {
		tagkey_length_ = 0;
		tagvalue_length_ = 0;
	}
};

//在TSI文件中被分配256个 从尾部开始写
//在TSM文件中位于indexblock中的filetrailer在4k的尾部
class SeriesKey {
public:
	friend class SeriesIndexBlock;
	friend class TSIFile;
	friend class SeriesSegement;
	friend class SeriesIndexToSegement;
	friend class TSMFile;

	SeriesKey();

	SeriesKey(uint16_t name_length,string name,uint32_t tag_num, vector<Tag> tags);

	~SeriesKey();

	bool operator==(const SeriesKey& series_key) const;
	bool operator!=(const SeriesKey& series_key) const;
	bool operator<(const SeriesKey& series_key) const;
	bool operator>(const SeriesKey& series_key) const;
	bool operator()(const SeriesKey& left, const SeriesKey& right) const;

	//新建一个SeriesKey是需要调用
	//从小到大排序tags
	void Init();

	void Init(char data[257]);

	string ToString();

	string GetName() { return name_; }
	vector<Tag> GetTags() { return tags_; }
	//返回对应index的tag
	Tag GetTag(int index) { return tags_[index]; }
	int GetNum() { return tag_num_; }
	int GetSize();

private:
	string name_;
	uint16_t name_length_;				//char[2]
	vector<Tag> tags_;
	uint16_t tag_num_;						//char[2]
};