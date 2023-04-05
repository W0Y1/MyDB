#pragma once
#include"config.h"
#include<string>
#include"Storer.h"

//�ܴ�С48
 struct IndexEntry {
	uint64_t min_time_;								//char[16]
	uint64_t max_time_;								//char[16]
	uint32_t offset_;									//char[8]
	uint32_t size_;										//block�Ĵ�С������offset+size���Կ��ٶ���		char[8]
};

 struct SeriesIndexFileTrailer {
	 Key key_;
	 char type_[8];										//��Ӧdatablock��data����������
	 uint16_t count_;								//index_entrys_������				char[2]

	 SeriesIndexFileTrailer() {
		 count_ = 0;
		 memcpy(type_, "", 0);
	 }
};

 //ÿ��SeriesIndexBlock��С�̶� 4KB	char[4096]
 //IndexEntry��ͷ����ʼд SeriesIndexFileTrailer��β����ʼд
class SeriesIndexBlock {
public:
	friend class TSMFile;

	SeriesIndexBlock();

	//�����ǰKey������ �µ�IndexBlock
	SeriesIndexBlock(const Key& key, const char type[8]);

	//����TSMFileTrailer�󣬸���offsetÿ4KBһ��SeriesIndexBlock
	void init(char data[4096]);

	//����ʱ���ʹ��max_time��min_time��λ������offset��size
	bool SearchDataBlock(uint64_t time, vector<pair<int, int>>& data);

	//����field_value����ʱ��������datablock
	bool SearchDataBlock(vector<pair<int, int>>& data);

	//������һ���µ�datablock����Ҫ����һ���µ�indexentry
	void AddNewIndexEntry(uint64_t min_time, uint64_t max_time, uint32_t size, uint32_t offset);

	//��SeriesDataBlock���������ݺ���Ҫ������ӦIndexEntry
	void Update(uint64_t min_time, uint64_t max_time, uint32_t change_size,uint32_t pos_entry);

	uint16_t GetCount() { return file_trailer_.count_; }

private:
	vector<IndexEntry> index_entrys_;

	SeriesIndexFileTrailer file_trailer_;
};