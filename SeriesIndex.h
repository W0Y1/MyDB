#pragma once
#include"config.h"

enum  IndexEntryType { INVALID_INDEX_ENTRY = 0, LEAF_ENTRY, INTERNAL_ENTRY };


//��ÿ��entry������ͬ��С�Ŀռ䣬��seriesblock�ϸ���id˳��洢��
//�˴�ΪHEADER ����4k��ͷ��
class SeriesIndex {
public:
	bool IsLeafEntry() const;
	bool IsRootEntry() const;
	void SetEntryType(IndexEntryType entry_type);

	int GetSize() const;
	void SetSize(int size);
	void IncreaseSize(int amount);

	int GetMaxSize() const;
	void SetMaxSize(int max_size);
	int GetMinSize() const;

	entry_id_t GetParentEntryId() const;
	void SetParentEntryId(entry_id_t parent_entry_id);

	entry_id_t GetEntryId() const;
	void SetEntryId(entry_id_t entry_id);

private:
	IndexEntryType entry_type_;					//char[1]
	int size_;													//char[2]
	int max_size_;											//���ô�
	entry_id_t entry_id_;									//���ô�
	entry_id_t parent_entry_id_;						//char[3]
};
