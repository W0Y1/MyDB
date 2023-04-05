#pragma warning(disable : 4996)
#include"Meta.h"

//time1超过time2返回0,反之
bool CompareTime(tm time1, tm time2) {
	if (time1.tm_year > time2.tm_year) return 0;
	if (time1.tm_year < time2.tm_year) return 1;

	if (time1.tm_mon > time2.tm_mon) return 0;
	if (time1.tm_mon < time2.tm_mon) return 1;

	if (time1.tm_mday > time2.tm_mday) return 0;
	if (time1.tm_mday < time2.tm_mday) return 1;

	if (time1.tm_hour > time2.tm_hour) return 0;
	if (time1.tm_hour < time2.tm_hour) return 1;

	if (time1.tm_min > time2.tm_min) return 0;
	if (time1.tm_min < time2.tm_min) return 1;

	if (time1.tm_sec > time2.tm_sec) return 0;
	return 1;
}



Meta::Meta() {
	path_ = PATH_FILE + "\\meta.txt";
	shard_count_ = 0;
}

Meta::~Meta(){}

bool Meta::Init() {
	db_io_.open(path_, ios::in | ios::binary | ios::out);
	if (!db_io_.is_open())return false;
	db_io_.seekg(0,ios::end);
	int size = db_io_.tellg();

	shard_count_ = size / 63;

	max_id_ = shard_count_ - 1;

	db_io_.seekg(ios::beg);
	char* data = new char[size + 1];
	db_io_.read(data, size);

	char tmp_1[2] = {};
	char tmp_4[5] = {};
	char tmp_2[3] = {};
	char tmp_16[17] = {};
	int offset = 0;

	time_t t = time(nullptr);
	struct tm* now = localtime(&t);
	for (int i = 0; i < size / 63; i++) {
		offset = i * 63;
		ShardInfo tmp;

		memcpy(tmp_1, data + offset, 1);
		offset += 1;
		if (tmp_1 == "1") {
			shard_count_--;
			continue;
		}
		tmp.tomb = 0;

		memcpy(tmp_4, data + offset, 4);
		offset += 4;
		tmp.id_ = atof(tmp_4);

		memcpy(tmp_16, data + offset, 16);
		offset += 16;
		tmp.owner_ = tmp_16;

		memcpy(tmp_4, data + offset, 4);
		offset += 4;
		tmp.start_time_.tm_year = atof(tmp_4);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.start_time_.tm_mon = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.start_time_.tm_mday = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.start_time_.tm_hour = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.start_time_.tm_min = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.start_time_.tm_sec = atof(tmp_2);

		memcpy(tmp_4, data + offset, 4);
		offset += 4;
		tmp.end_time_.tm_year = atof(tmp_4);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.end_time_.tm_mon = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.end_time_.tm_mday = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.end_time_.tm_hour = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.end_time_.tm_min = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.end_time_.tm_sec = atof(tmp_2);

		memcpy(tmp_4, data + offset, 4);
		offset += 4;
		tmp.delete_time_.tm_year = atof(tmp_4);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.delete_time_.tm_mon = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.delete_time_.tm_mday = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.delete_time_.tm_hour = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.delete_time_.tm_min = atof(tmp_2);
		memcpy(tmp_2, data + offset, 2);
		offset += 2;
		tmp.delete_time_.tm_sec = atof(tmp_2);

		//检查delete_time_是否超过当前时间
		if (!CompareTime(*now, tmp.delete_time_)) {
			listFiles(PATH_FILE + "\\" + tmp.owner_ + "\\shard" + to_string(tmp.id_));
			shard_count_--;
			db_io_.seekg(i * 63);
			db_io_.write("1", 1);
			continue;
		}
		DatabaseShardMap_[tmp.owner_].push_back(tmp);
	}
	delete[]data;
	db_io_.close();
	return true;
}

int Meta::CreateNewShard(string owner, tm start_time) {
	 ShardInfo new_shardinfo;
	 new_shardinfo.id_ = max_id_ + 1;
	 new_shardinfo.tomb = 0;
	 max_id_ += 1;
	 shard_count_ + 1;
	 new_shardinfo.owner_ = owner;
	 new_shardinfo.start_time_ = start_time;

	 tm end_time = start_time;
	 if (start_time.tm_mday != 31) {
		 end_time.tm_mday += 2;
	 }
	 end_time.tm_hour = 23;
	 end_time.tm_min = 59;
	 end_time.tm_sec = 59;

	 new_shardinfo.end_time_ = end_time;
	 new_shardinfo.delete_time_ = start_time;
	 new_shardinfo.delete_time_.tm_year += 1;

	 DatabaseShardMap_[owner].push_back(new_shardinfo);

	 char* data = new char[64];
	 int offset = 0;
	 char tmp_4[5] = {};
	 char tmp_16[17] = {};
	 memcpy(data + offset, to_string(new_shardinfo.tomb).c_str(), 1);
	 offset += 1;
	 memcpy(data + offset, to_string(new_shardinfo.id_).c_str(), 4);
	 offset += 4;
	 memcpy(data + offset, new_shardinfo.owner_.c_str(), 16);
	 offset += 16;
	 memcpy(data + offset, to_string(new_shardinfo.start_time_.tm_year).c_str(), 4);
	 offset += 4;
	 memcpy(data + offset, to_string(new_shardinfo.start_time_.tm_mon).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.start_time_.tm_mday).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.start_time_.tm_hour).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.start_time_.tm_min).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.start_time_.tm_sec).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.end_time_.tm_year).c_str(), 4);
	 offset += 4;
	 memcpy(data + offset, to_string(new_shardinfo.end_time_.tm_mon).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.end_time_.tm_mday).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.end_time_.tm_hour).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.end_time_.tm_min).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.end_time_.tm_sec).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.delete_time_.tm_year).c_str(), 4);
	 offset += 4;
	 memcpy(data + offset, to_string(new_shardinfo.delete_time_.tm_mon).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.delete_time_.tm_mday).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.delete_time_.tm_hour).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.delete_time_.tm_min).c_str(), 2);
	 offset += 2;
	 memcpy(data + offset, to_string(new_shardinfo.delete_time_.tm_sec).c_str(), 2);
	 offset += 2;

	 db_io_.open(path_, ios::out | ios::binary | ios::app | ios::in);
	 if (!db_io_.is_open()) {
		 db_io_.open(path_, ios::out | ios::binary | ios::app);
	 }
	 db_io_.write(data, 63);
	 db_io_.close();

	 return new_shardinfo.id_;
 }

int Meta::GetShard(string database, tm time, bool flag) {
	for (auto iter = DatabaseShardMap_[database].begin(); iter != DatabaseShardMap_[database].end(); iter++) {
		if (CompareTime(time, iter->start_time_) == 0 && CompareTime(time, iter->end_time_) == 1) {
			return iter->id_;
		}
	}
	if (!flag)return-1;
	tm start_time = time;
	start_time.tm_hour = 0;
	start_time.tm_min = 0;
	start_time.tm_sec = 0;
	start_time.tm_mday = ((start_time.tm_mday) / 3) * 3;

	return CreateNewShard(database, start_time);
}

void Meta::GetShard(string database, vector<int>& ids) {
	for (int i = 0; i < DatabaseShardMap_[database].size(); i++) {
		ids.push_back(DatabaseShardMap_[database][i].id_);
	}
}

vector<ShardInfo> Meta::GetAllShardInfo() {
	vector<ShardInfo>result;
	for (auto const& p : DatabaseShardMap_) {
		result.insert(result.end(), p.second.begin(), p.second.end());
	}
	return result;
}