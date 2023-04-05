#include"BufferPool.h"

BufferPool::BufferPool(size_t pool_size)
	:pool_size_(pool_size) {
	seriesids_ = new SeriesId[pool_size];
	replacer_ = new LRUReplacer(pool_size);

	for (size_t i = 0; i < pool_size_; ++i) {
		free_list_.emplace_back(static_cast<int>(i));
	}
}

BufferPool::~BufferPool() {
	delete[] seriesids_;
	delete replacer_;
}

void BufferPool::erase_table(string name, string tagkey, string tagvalue, frame_id_t frame_id) {
	auto iter = series_table_[name][tagkey][tagvalue].begin();
	for (; iter != series_table_[name][tagkey][tagvalue].end(); iter++) {
		if (*iter == frame_id) break;
	}
	series_table_[name][tagkey][tagvalue].erase(iter);

	if (!series_table_[name][tagkey][tagvalue].size()) {
		series_table_[name][tagkey].erase(tagvalue);

		if (!series_table_[name].count(tagkey)) {
			series_table_[name].erase(tagkey);

			if (!series_table_.count(name)) {
				series_table_.erase(name);
			}
		}
	}
	return;
}

bool BufferPool::find_replace(frame_id_t* frame_id, TSIFile* tsi) {
	if (!free_list_.empty()) {
		*frame_id = free_list_.front();
		free_list_.pop_front();
		return true;
	}

	if (replacer_->Victim(frame_id)) {
		SeriesKey serieskey = series_keys_[seriesids_[*frame_id]];
		//向tsi文件写新的SeriesKey
		tsi->AddNewSeriesKey(serieskey, seriesids_[*frame_id]);

		//清除相应数据
		series_keys_.erase(seriesids_[*frame_id]);

		vector<Tag> tags = serieskey.GetTags();
		for (int i = 0; i < serieskey.GetNum(); i++) {
			erase_table(serieskey.GetName(), tags[i].tagkey_, tags[i].tagvalue_, *frame_id);
		}
		return true;
	}
	return false;
}

void BufferPool::FetchSeriesKey(string name, string tagkey, string tagvalue, vector<SeriesKey>& keys) {
	latch_.lock();

	list<frame_id_t> tmp = series_table_[name][tagkey][tagvalue];
	for (auto iter = tmp.begin(); iter != tmp.end(); iter++) {
		replacer_->Pin(*iter);
		keys.push_back(series_keys_[seriesids_[*iter]]);
		UnpinSeriesId(*iter);
	}
	latch_.unlock();
	return;
}

bool BufferPool::LoadNewSeriesKey(SeriesKey& series_key, SeriesId seriesid, TSIFile* tsi) {
	latch_.lock();
	frame_id_t victim_id;
	if (!find_replace(&victim_id,tsi)) {
		latch_.unlock();
		return false;
	}
	seriesids_[victim_id] = seriesid;
	series_keys_[seriesid] = series_key;

	for (int i = 0; i < series_key.GetNum(); i++) {
		Tag tag = series_key.GetTag(i);
		if (series_table_.count(series_key.GetName()) != 0) {
			if (series_table_[series_key.GetName()].count(tag.tagkey_) != 0) {
				if (series_table_[series_key.GetName()][tag.tagkey_].count(tag.tagvalue_) != 0) {
					series_table_[series_key.GetName()][tag.tagkey_][tag.tagvalue_].push_back(victim_id);
				}
				else {
					list<frame_id_t> tmp_list;
					series_table_[series_key.GetName()][tag.tagkey_][tag.tagvalue_] = tmp_list;
					series_table_[series_key.GetName()][tag.tagkey_][tag.tagvalue_].push_back(victim_id);
				}
			}
			else {
				unordered_map<string, list<frame_id_t>> tmp;
				list<frame_id_t> tmp_list;
				tmp[tag.tagvalue_] = tmp_list;
				tmp[tag.tagvalue_].push_back(victim_id);
				series_table_[series_key.GetName()][tag.tagkey_] = tmp;
			}
		}
		else {
			unordered_map<string, list<frame_id_t>> tmp_tagvalue;
			list<frame_id_t> tmp_list;
			tmp_tagvalue[tag.tagvalue_] = tmp_list;
			tmp_tagvalue[tag.tagvalue_].push_back(victim_id);
			unordered_map<string, unordered_map<string, list<frame_id_t>>>tmp_tagkey;
			tmp_tagkey[tag.tagkey_] = tmp_tagvalue;
			series_table_[series_key.GetName()] = tmp_tagkey;
			
		}
	}
	replacer_->Unpin(victim_id);
	latch_.unlock();
	return true;
}

bool BufferPool::UnpinSeriesId(int frame_ids) {
	latch_.lock();

	replacer_->Unpin(frame_ids);

	latch_.unlock();
	return true;
}

bool BufferPool::FlushAllSeriesKey(TSIFile* tsi){
	latch_.lock();
	if (series_keys_.size() == 0) {
		latch_.unlock();
		return false;
	}
	for (const auto& p : series_keys_) {
		tsi->AddNewSeriesKey(p.second, p.first);
	}
	latch_.unlock();
	return true;
}

SeriesId BufferPool::GetSeriesId(int frame_ids) { 
	UnpinSeriesId(frame_ids);
	return seriesids_[frame_ids]; 	
}

bool BufferPool::IsExist(string name, string tagkey, string tagvalue) {
	if (series_table_.count(name) != 0) {
		if (series_table_[name].count(tagkey) != 0) {
			if (series_table_[name][tagkey].count(tagvalue) != 0) {
				return true;
			}
		}
	}
	return false;
}