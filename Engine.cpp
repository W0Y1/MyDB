#include"Engine.h"

Engine::Engine(SeriesFile* seriesfile) {
	id_ = -1;
	sfile_ = seriesfile;
	cache_ = new Cache;
}

Engine::Engine(int id, string path,SeriesFile *seriesfile) {
	id_ = id;
	path_ = path;
	cache_ = new Cache;
	getAllFiles(path, tsm_files_);
	file_num_ = tsm_files_.size();
	for (int i = 0; i < file_num_; i++) {
		compact_.push_back(new Compact());
		compact_[i]->SetPath(tsm_files_[i]);
		compact_[i]->SetId(id);
	}
	sfile_ = seriesfile;
}

Engine::~Engine() {
	delete cache_;
	for (auto iter = compact_.begin(); iter != compact_.end(); iter++) {
		delete *iter;
	}
}

void Engine::SetPath(string path) {
	path_ = path;
	_mkdir(path.c_str());
	getAllFiles(path, tsm_files_);
	file_num_ = tsm_files_.size();
	for (int i = 0; i < file_num_; i++) {
		compact_.push_back(new Compact());
		compact_[i]->SetPath(tsm_files_[i]);
		compact_[i]->SetId(i);
	}
}

void Engine::WriteToCache(SeriesKey& serieskey, string field_key, string field_value, string value_type, uint64_t timestamp) {
	SeriesKey tmp = serieskey;
	Key new_key(tmp,field_key);
	if (!cache_->AddData(new_key, field_value, value_type, timestamp)) {
		map<Key, Values>data;
		cache_->FlushToFile(data);
		WriteToFile(data, path_ + "\\tsm" + to_string(file_num_++)+".txt");
	}
}

void Engine::WriteToFile(map<Key, Values>& data, string path) {
	Compact* compact = new Compact(path, id_);
	compact->Write(data);
	compact_.push_back(compact);
	tsm_files_.push_back(path);
	file_num_++;
}

void Engine::FlushToDisk() {
	string path = path_ + "\\tsm" + to_string(file_num_++) + ".txt";
	map<Key, Values> data;
	if (!cache_->FlushToFile(data)) return;
	WriteToFile(data, path);
}

bool Engine::SearchData(SeriesKey serieskey, string field_key, vector<uint64_t>& times ,vector<string>& values) {
	Key key(serieskey,field_key);

	bool flag=cache_->SearchData(key, values, times);
	//bool flag_1= compact_[0]->SearchData(key, times, values);
	bool flag_1;
	thread t1(bind(&Engine::CreatTheadToSearch,this, key, ref(times), ref(values), 0, file_num_ / 4, &flag_1));
	bool flag_2;
	thread t2(bind(&Engine::CreatTheadToSearch, this, key, ref(times), ref(values), file_num_ / 4, file_num_ / 2, &flag_2));
	bool flag_3;
	thread t3(bind(&Engine::CreatTheadToSearch, this, key, ref(times), ref(values), file_num_ / 2, (file_num_ / 4) * 3, &flag_3));
	bool flag_4;
	thread t4(bind(&Engine::CreatTheadToSearch, this, key, ref(times), ref(values), (file_num_ / 4) * 3, file_num_, &flag_4));
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	return flag || flag_1||flag_2||flag_3||flag_4;
}

bool Engine::SearchDataByTime(SeriesKey serieskey, string field_key, uint64_t timestamp, vector<string>& values) {
	Key key(serieskey,field_key);

	bool flag = cache_->SearchData(key, timestamp, values);
	bool flag_1;
	thread t1(bind(&Engine::CreatTheadToSearchByTime, this, key, timestamp, ref(values), 0, file_num_ / 4, &flag_1));
	bool flag_2;
	thread t2(bind(&Engine::CreatTheadToSearchByTime, this, key, timestamp, ref(values), file_num_ / 4, file_num_ / 2, &flag_2));
	bool flag_3;
	thread t3(bind(&Engine::CreatTheadToSearchByTime, this, key, timestamp, ref(values), file_num_ / 2, (file_num_ / 4) * 3, &flag_3));
	bool flag_4;
	thread t4(bind(&Engine::CreatTheadToSearchByTime, this, key, timestamp, ref(values), (file_num_ / 4) * 3, file_num_, &flag_4));
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	flag |= flag_1 | flag_2 | flag_3 | flag_4;
	return flag;
}

void Engine::CreatTheadToSearchByTime(Key key, uint64_t timestamp, 
	vector<string>& values, int low, int high, bool* flag){
	for (int i = low; i < high; i++) {
		*flag |= compact_[i]->SearchDataByTime(key, timestamp, values);
	}
}

void Engine::CreatTheadToSearch(Key key, vector<uint64_t>& times, vector<string>& values, int low, int high, bool* flag) {
	for (int i = low; i < high; i++) {
		compact_[i]->SearchData(key, times, values);
	}
}