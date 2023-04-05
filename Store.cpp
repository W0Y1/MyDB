#pragma warning(disable : 4996)
#include"Store.h"
#include<functional>

Store::Store() {
	path_ = PATH_FILE;
	data_ = new Meta;
	vector<string> databases;
	getAllFilesName(path_, databases);
	for (int i = 0; i < databases.size(); i++) {
		if (databases[i] == "meta.txt")continue;
		database_status[databases[i]] = 1;
		SeriesFile* new_seriesfile = new SeriesFile(path_ + "\\" + databases[i] + "\\seriesfile");
		series_files_[databases[i]] = new_seriesfile;
	}
}

Store::~Store() {
	delete data_;
	for (auto iter = shards_.begin(); iter != shards_.end(); iter++) {
		delete iter->second;
	}
	for (auto iter = series_files_.begin(); iter != series_files_.end(); iter++) {
		delete iter->second;
	}
}

bool Store::Init() {
	if (!data_->Init()) return false;

	unordered_map<string, vector<ShardInfo>> shardinfo = data_->GetAllShard();
	for (auto iter = shardinfo.begin(); iter != shardinfo.end(); iter++) {
		for (int i = 0; i < iter->second.size(); i++) {
			Shard* new_shard = new Shard(series_files_[iter->first]);
			new_shard->SetId(iter->second[i].id_);
			new_shard->SetDatabase(iter->second[i].owner_);
			new_shard->SetPath(path_ + "\\" + iter->second[i].owner_ + "\\shard" + to_string(iter->second[i].id_));
			shards_[iter->second[i].id_] = new_shard;
		}
	}
	return true;
}

bool Store::SetCurDatabase(string database) {
	if (database_status.count(database) == 0) return false;

	cur_database_ = database;
	return true;
}

bool Store::CreateNewDatabase(string database) {
	if (series_files_.count(database) != 0) return false;
	_mkdir((path_ + "\\" + database).c_str());
	database_status[database] = 1;
	series_files_[database] = new SeriesFile(path_ + "\\" + database + "\\seriesfile");
	return true;
}

void Store::ShowDatabases(vector<string>& databases) {
	for (auto const& p : database_status) {
		databases.push_back(p.first);
	}
}

bool Store::InsertData(SeriesKey& serieskey, string field_key, string field_value, string value_type, const long long timestamp) {
	if (cur_database_.length() == 0) return false;
	int id = data_->GetShard(cur_database_, *gmtime(&timestamp), 1);
	if (shards_.count(id) == 0) {
		Shard* new_shard = new Shard(series_files_[cur_database_]);
		new_shard->SetId(id);
		new_shard->SetDatabase(cur_database_);
		new_shard->SetPath(path_ + "\\" + cur_database_ + "\\shard" + to_string(id));

		shards_[id] = new_shard;
	}
	return shards_[id]->WriteData(serieskey, field_key, field_value, value_type, timestamp);
}

bool Store::SearchData(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times, vector<string>& group_info) {
	if (cur_database_.empty()) return false;
	vector<int> ids;
	data_->GetShard(cur_database_, ids);
	if (ids.empty())return false;

	int flag_1;
	thread t1(bind(&Store::CreateThreadToSearch, this, plan, ref(result_values), ref(result_times), ref(group_info), ids, 0, ids.size() / 2, &flag_1));
	int flag_2;
	thread t2(bind(&Store::CreateThreadToSearch, this, plan, ref(result_values), ref(result_times), ref(group_info), ids, ids.size() / 2, ids.size(), &flag_2));
	t1.join();
	t2.join();
	//bool flag = shards_[0]->SearchData(plan, result_values, result_times, group_info);
	return flag_1||flag_2;
}

bool Store::SearchDataByTime(Plan* plan, const long long timestamps, vector<string>& result, vector<string>& group_info) {
	if (cur_database_.empty()) return false;
	int id = data_->GetShard(cur_database_, *gmtime(&timestamps), 0);
	if (id == -1)return false;
	return shards_[id]->SearchDataByTime(plan, timestamps, result, group_info);
}

void Store::CreateThreadToSearch(Plan* plan, vector<string>& result_values, vector<uint64_t>& result_times, vector<string>& group_info
	, vector<int> ids, int low, int high, int* flag) {
	*flag = 0;
	for (int i = low; i < high; i++) {
		*flag |= shards_[ids[i]]->SearchData(plan, result_values, result_times, group_info);
		if (*flag == false) {
			cout << "groupbyÊäÈë´íÎó" << endl;
			break;
		}
	}
}

void Store::FlushToDisk() {
	vector<thread> threads;
	for (auto iter = series_files_.begin(); iter != series_files_.end(); iter++) {
		threads.push_back(thread(bind(&SeriesFile::Flush, iter->second)));
	}
	//series_files_["w0y1"]->Flush();
	for (auto iter = shards_.begin(); iter != shards_.end(); iter++) {
		threads.push_back(thread(bind(&Store::CreateThreadToFlush, this, iter->first)));
	}

	//CreateThreadToFlush(1);
	for (auto iter = threads.begin(); iter != threads.end(); iter++) {
		iter->join();
	}
}
