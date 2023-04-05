#include"Index.h"
#include<functional>

Index::Index(SeriesFile* sfile) {
	sfile_ = sfile;
	bufferpool_ = new BufferPool(POOL_SIZE);
}

Index::Index(string path, string database, SeriesFile* sfile) {
	path_ = path;
	database_ = database;
	sfile_ = sfile;

	bufferpool_ = new BufferPool(POOL_SIZE);
	vector<string> files_name;
	getAllFiles(path_, files_name);
	tsi_ = new TSIFile[files_name.size() + 1];
	tsi_num_ = files_name.size();
	for (int i = 0; i < files_name.size(); i++) {
		tsi_[i].SetPath(files_name[i]);
		tsi_[i].SetId(i);
		tsi_[i].Init();
	}
	tsi_[files_name.size()].SetId(files_name.size());
	tsi_[files_name.size()].SetPath(path_ + "\\tsi" + to_string(files_name.size())+".txt");
}

Index::~Index() {
	delete bufferpool_;
}

void Index::SetPath(string path) {
	path_ = path;
	_mkdir(path.c_str());
	vector<string> files_name;
	getAllFiles(path_, files_name);

	tsi_ = new TSIFile[files_name.size() + 1];
	tsi_num_ = files_name.size();
	for (int i = 0; i < files_name.size(); i++) {
		tsi_[i].SetPath(files_name[i]);
		tsi_[i].SetId(i);
		tsi_[i].Init();
	}
	tsi_[files_name.size()].SetId(files_name.size());
	tsi_[files_name.size()].SetPath(path_ + "\\tsi" + to_string(files_name.size()) + ".txt");
}


vector<SeriesKey> Index::SearchSeriesKey(string name, string tag_key, string tag_value) {
	vector<SeriesKey> result;
	result.reserve(4096);
	bufferpool_->FetchSeriesKey(name, tag_key, tag_value, result);

	vector<SeriesId> seriesids;
	//seriesids.reserve(4096);
	//thread t1(bind(&Index::CreateThreadToSearch, this, name, tag_key, tag_value, 0, tsi_num_ / 4, ref(seriesids)));
	//thread t2(bind(&Index::CreateThreadToSearch, this, name, tag_key, tag_value, tsi_num_ / 4, tsi_num_ / 2, ref(seriesids)));
	//thread t3(bind(&Index::CreateThreadToSearch, this, name, tag_key, tag_value, tsi_num_ / 2, (tsi_num_ / 4) * 3, ref(seriesids)));
	//thread t4(bind(&Index::CreateThreadToSearch, this, name, tag_key, tag_value, (tsi_num_ / 4) * 3, tsi_num_, ref(seriesids)));
	//t1.join();
	//t2.join();
	//t3.join();
	//t4.join();
	seriesids= tsi_[0].FindSeriesId(name, tag_key, tag_value);
	sfile_->GetSeriesKey(result, seriesids);
	return result;
}

void Index::CreateThreadToSearch(string name, string tag_key, string tag_value, int low, int high, vector<SeriesId>& seriesids) {
	for (int i = low; i < high; i++) {
		vector<SeriesId>tmp = tsi_[i].FindSeriesId(name, tag_key, tag_value);
		seriesids.insert(seriesids.end(), tmp.begin(), tmp.end());
	}
}

bool Index::IsExistSeriesKey(SeriesKey& serieskey) {
	bool flag = 1;
	for (int i = 0; i < serieskey.GetNum(); i++) {
		Tag tmp_tag = serieskey.GetTag(i);
		flag &= bufferpool_->IsExist(serieskey.GetName(), tmp_tag.tagkey_, tmp_tag.tagvalue_);
	}
	if (flag == 1) return true;

	flag = 0;
	bool flag_1 = 0, flag_2 = 0, flag_3 = 0, flag_4 = 0;
	//flag |= tsi_[0].SearchSeriesKey(serieskey);
	thread t1(bind(&Index::CreateThreadToJudge, this, ref(serieskey), 0, tsi_num_ / 4, ref(flag_1)));
	thread t2(bind(&Index::CreateThreadToJudge, this, ref(serieskey), tsi_num_ / 4, (tsi_num_ / 4) * 2, ref(flag_2)));
	thread t3(bind(&Index::CreateThreadToJudge, this, ref(serieskey), (tsi_num_ / 4) * 2, (tsi_num_ / 4) * 3, ref(flag_3)));
	thread t4(bind(&Index::CreateThreadToJudge, this, ref(serieskey), (tsi_num_ / 4) * 3, tsi_num_, ref(flag_4)));
	t1.join();
	t2.join();
	t3.join();
	t4.join();

	flag |= flag_1 | flag_2 | flag_3 | flag_4;
	if (flag == 1) return true;

	AddNewSeriesKey(serieskey);
	return false;
}

void Index::CreateThreadToJudge(SeriesKey& serieskey, int low, int high, bool& flag) {
	for (int i = low; i < high; i++) {
		flag |= tsi_[i].SearchSeriesKey(serieskey);
		if (flag == 1) break;
	}
}

void Index::AddNewSeriesKey(SeriesKey& serieskey) {
	SeriesId seriesid = sfile_->IfExist(serieskey);
	bufferpool_->LoadNewSeriesKey(serieskey, seriesid, &tsi_[tsi_num_]);
}

bool Index::FlushToDisk() {
	if (!bufferpool_->FlushAllSeriesKey(&tsi_[tsi_num_])) return false;
	bool flag = tsi_[tsi_num_].FlushToFile();
	delete[]tsi_;
	vector<string> files_name;
	getAllFiles(path_, files_name);

	tsi_ = new TSIFile[files_name.size() + 1];
	tsi_num_ = files_name.size();
	for (int i = 0; i < files_name.size(); i++) {
		tsi_[i].SetPath(files_name[i]);
		tsi_[i].SetId(i);
		tsi_[i].Init();
	}
	tsi_[files_name.size()].SetId(files_name.size());
	tsi_[files_name.size()].SetPath(path_ + "\\tsi" + to_string(files_name.size()) + ".txt");

}