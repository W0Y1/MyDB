#pragma once
#include<string>
using std::string;
using std::vector;

class Plan {
public:
	Plan() { flag_timestamp_ = 0; }

	void SetMeasurement(string measurement) {measurement_ = measurement;}

	void SetTagKey(string tagkey) { tagkey_.push_back(tagkey); }

	void SetTagValue(string tagvalue) { tagvalue_.push_back(tagvalue); }

	void AddSelect(string fieldkey) { select_.push_back(fieldkey); }

	void SetTime(uint64_t timestamp) {
		timestamp_ = timestamp;
		flag_timestamp_ = 1;
	}

	void SetGroup(string tagkey) { groupby_ = tagkey; }

	void clear() {
		measurement_.clear();
		tagkey_.clear();
		tagvalue_.clear();
		select_.clear();
		flag_timestamp_ = 0;
		groupby_.clear();
	}

	string measurement_;
	vector<string> tagkey_;
	vector<string> tagvalue_;
	vector<string> select_;
	bool flag_timestamp_;
	uint64_t timestamp_;

	string groupby_;
};