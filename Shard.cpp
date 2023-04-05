#include"Shard.h"
#include<functional>


bool Group(vector<SeriesKey> keys, string group_tagkey, vector<vector<SeriesKey>>& group_keys, vector<string>& group_info) {
	unordered_map<string, int> pos_hash;
	int index = -1;
	int size = 0;
	for (int i = 0; i < keys[0].GetNum(); i++) {
		Tag tag = keys[0].GetTag(i);
		if (tag.tagkey_ == group_tagkey) {
			index = i;
			group_keys[0].push_back(keys[0]);
			pos_hash[tag.tagvalue_] = 0;
			group_info.push_back(tag.tagvalue_);
			break;
		}
	}
	if (index == -1) return false;

	for (int i = 1; i < keys.size(); i++) {
		Tag tag = keys[i].GetTag(index);

		if (pos_hash.count(tag.tagvalue_) == 0) {
			int pos = group_keys.size();
			group_info.push_back(tag.tagvalue_);
			group_keys[pos].push_back(keys[i]);
			pos_hash[tag.tagvalue_] = pos;
		}
		else {
			group_keys[pos_hash[tag.tagvalue_]].push_back(keys[i]);
		}
	}
	return true;
}

Shard::Shard() {
	id_ = -1;
	is_dirty = false;
}

Shard::Shard(SeriesFile* seriesfile) {
	id_ = -1;
	index_ = new Index(seriesfile);
	engine_ = new Engine(seriesfile);
	is_dirty = false;
}

Shard::~Shard() {
	delete index_;
	delete engine_;
}

void Shard::SetPath(string path) {
	path_ = path;
	_mkdir(path.c_str());
	index_->SetPath(path + "\\index");
	engine_->SetPath(path + "\\tsm");
}

bool Shard::WriteData(SeriesKey& serieskey, string field_key, string field_value, string value_type, uint64_t timestamp) {
	index_->IsExistSeriesKey(serieskey);

	engine_->WriteToCache(serieskey, field_key, field_value, value_type, timestamp);

	is_dirty = true;
	return true;
}

bool Shard::SearchData(Plan* plan,vector<string>& result_values, vector<uint64_t>& result_times, vector<string>&group_info) {
	vector<SeriesKey> keys;
	unordered_map<string, int> keymap;
	if (plan->tagkey_.size() == 1) {
		keys = index_->SearchSeriesKey(plan->measurement_, plan->tagkey_[0], plan->tagvalue_[0]);
	}
	else {
		for (int i = 0; i < plan->tagkey_.size(); i++) {
			vector<SeriesKey> tmp = index_->SearchSeriesKey(plan->measurement_, plan->tagkey_[i], plan->tagvalue_[i]);
			if (i == 0) {
				for (int j = 0; j < tmp.size(); j++) {
					keymap[tmp[j].ToString()] = 1;
				}
			}
			else if (i < plan->tagkey_.size() - 1) {
				for (int j = 0; j < tmp.size(); j++) {
					if (keymap.count(tmp[j].ToString()) == 0)continue;
					keymap[tmp[j].ToString()]++;
				}
			}
			else {
				for (int j = 0; j < tmp.size(); j++) {
					if (keymap.count(tmp[j].ToString()) == 0)continue;
					if (keymap[tmp[j].ToString()] == plan->tagkey_.size() - 1)keys.push_back(tmp[j]);
				}
			}
		}
	}

	vector<vector<SeriesKey>> group_result;
	if (plan->groupby_.empty()) group_result.push_back(keys);
	else if(!Group(keys,plan->groupby_, group_result, group_info)) return false;

	//engine_->SearchData(keys[0], plan->select_[0], result_times, result_values);
	vector<thread> threads;
	vector<vector<string>>tmp_result_value;
	vector<vector<uint64_t>>tmp_result_time;
	for (int j = 0; j < plan->select_.size(); j++) {
		for (int i = 0; i < group_result.size(); i++) {
			vector<string>tmp_values;
			vector<uint64_t>tmp_times;
			tmp_result_value.push_back(tmp_values);
			tmp_result_time.push_back(tmp_times);
			tmp_result_value[i].reserve(4096);
			tmp_result_time[i].reserve(4096);
			threads.push_back(thread(bind(&Shard::CreateThreadToSearch, this, group_result[i], plan->select_[j], ref(tmp_result_value[i]), ref(tmp_result_time[i]))));
		}
		int count = 0;
		for (auto iter = threads.begin(); iter != threads.end(); iter++) {
			iter->join();
			result_values.insert(result_values.end(), tmp_result_value[count].begin(), tmp_result_value[count].end());
			result_values.push_back("000");
			result_times.insert(result_times.end(), tmp_result_time[count].begin(), tmp_result_time[count].end());
			result_times.push_back(000);
			count++;
		}
		threads.clear();
	}
	return true;
}

bool Shard::SearchDataByTime(Plan* plan, uint64_t timestamps, vector<string>& result, vector<string>& group_info) {
	vector<SeriesKey> keys;
	unordered_map<string, int> keymap;
	for (int i = 0; i < plan->tagkey_.size(); i++) {
		vector<SeriesKey> tmp = index_->SearchSeriesKey(plan->measurement_, plan->tagkey_[i], plan->tagvalue_[i]);
		if (i == 0) {
			for (int j = 0; j < tmp.size(); j++) {
				keymap[tmp[j].ToString()] = 1;
			}
		}
		else if (i < plan->tagkey_.size() - 1) {
			for (int j = 0; j < tmp.size(); j++) {
				if (keymap.count(tmp[j].ToString()) == 0)continue;
				keymap[tmp[j].ToString()]++;
			}
		}
		else {
			for (int j = 0; j < tmp.size(); j++) {
				if (keymap.count(tmp[j].ToString()) == 0)continue;
				if (keymap[tmp[j].ToString()] == plan->tagkey_.size() - 1)keys.push_back(tmp[j]);
			}
		}
	}

	vector<vector<SeriesKey>> group_result;
	if (plan->groupby_.empty()) group_result.push_back(keys);
	else if (!Group(keys, plan->groupby_, group_result, group_info)) return false;

	vector<thread> threads;
	vector<vector<string>> tmp_values;
	for (int j = 0; j < plan->select_.size(); j++) {
		for (int i = 0; i < group_result.size(); i++) {
			vector<string>tmp;
			tmp_values.push_back(tmp);
			tmp_values[i].reserve(4096);
			threads.push_back(thread(bind(&Shard::CreateThreadToSearchByTime, this, group_result[i], plan->select_[j], timestamps, ref(tmp_values[i]))));
		}
		int count = 0;
		for (auto iter = threads.begin(); iter != threads.end(); iter++) {		
			iter->join();
			result.insert(result.end(), tmp_values[count].begin(), tmp_values[count].end());
			result.push_back("000");
			count++;
		}
		threads.clear();
	}
	return true;
}

void Shard::CreateThreadToSearch(vector<SeriesKey>keys, string field_key, vector<string>& values, vector<uint64_t>& times) {
	for (int i = 0; i < keys.size(); i++) {
		engine_->SearchData(keys[i], field_key, times, values);
	}
}

void Shard::CreateThreadToSearchByTime(vector<SeriesKey>keys, string field_key, uint64_t timestamp, vector<string>& values) {
	for (int i = 0; i < keys.size(); i++) {
		engine_->SearchDataByTime(keys[i], field_key, timestamp, values);
	}
}

void Shard::FlushToDisk() {
	if (is_dirty) {
		index_->FlushToDisk();

		engine_->FlushToDisk();
		is_dirty = false;
	}
}