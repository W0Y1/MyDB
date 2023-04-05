#include"SeriesIndex.h"

bool SeriesIndex::IsLeafEntry() const {
    if (entry_type_ == LEAF_ENTRY) {
        return true;
    }
    return false;
}
bool SeriesIndex::IsRootEntry() const {
    if (parent_entry_id_ == INVALID_ENTRY_ID && entry_type_ == INTERNAL_ENTRY) {
        return true;
    }
    return false;
}

void SeriesIndex::SetEntryType(IndexEntryType entry_type) { entry_type_ = entry_type; }


int SeriesIndex::GetSize() const { return size_; }
void SeriesIndex::SetSize(int size) { size_ = size; }
void SeriesIndex::IncreaseSize(int amount) { size_ += amount; }


int SeriesIndex::GetMaxSize() const { return max_size_; }
void SeriesIndex::SetMaxSize(int size) { max_size_ = size; }


int SeriesIndex::GetMinSize() const { return max_size_ / 2; }


entry_id_t SeriesIndex::GetParentEntryId() const { return parent_entry_id_; }
void SeriesIndex::SetParentEntryId(entry_id_t parent_entry_id) { parent_entry_id_ = parent_entry_id; }


entry_id_t SeriesIndex::GetEntryId() const { return entry_id_; }
void SeriesIndex::SetEntryId(entry_id_t entry_id) { entry_id_ = entry_id; }