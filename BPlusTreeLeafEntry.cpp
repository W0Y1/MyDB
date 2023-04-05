#include "BPlusTreeLeafEntry.h"

BPlusTreeLeafEntry::~BPlusTreeLeafEntry() {
    delete[]serieskeys;
}


void BPlusTreeLeafEntry::Init(entry_id_t entry_id, entry_id_t parent_id, int max_size) {
    SetEntryId(entry_id);
    SetParentEntryId(parent_id);
    SetMaxSize(max_size);
    SetEntryType(LEAF_ENTRY);
    SetSize(0);
    serieskeys = new SeriesKey[20];
}

/**
 * Helper methods to set/get next entry id
 */

entry_id_t BPlusTreeLeafEntry::GetNextEntryId() const { return next_entry_id_; }


void BPlusTreeLeafEntry::SetNextEntryId(entry_id_t next_entry_id) { next_entry_id_ = next_entry_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */

int BPlusTreeLeafEntry::KeyIndex(const SeriesKey& key) const {
    int l = 0;
    int r = GetSize();
    if (l >= r) {
        return r;
    }
    while (l < r) {
        int mid = (l + r) / 2;
        if (serieskeys[mid]< key) {
            l = mid + 1;
        }
        else if (serieskeys[mid]== key) {
            return mid;
        }
        else {
            r = mid;
        }
    }
    return l;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */

SeriesKey BPlusTreeLeafEntry::KeyAt(int index) const {
    // replace with your own code
    return serieskeys[index];

}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */

const SeriesKey& BPlusTreeLeafEntry::GetItem(int index) {
    // replace with your own code
    return serieskeys[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /*
  * Insert key & value pair into leaf entry ordered by key
  * @return  entry size after insertion
  */

int BPlusTreeLeafEntry::Insert(const SeriesKey& key) {
    if (GetSize() == GetMaxSize()) {
        std::cout << "leaf_entry Insert: size=" << GetSize() << ", max_size=" << GetMaxSize() << std::endl;
    }
    int pos = KeyIndex(key);
    for (int i = GetSize() - 1; i >= pos; i--) {
        serieskeys[i + 1] = serieskeys[i];
    }
    serieskeys[pos] = key;
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
 /*
  * Remove half of key & value pairs from this entry to "recipient" entry
  */

void BPlusTreeLeafEntry::MoveHalfTo(BPlusTreeLeafEntry* recipient) {
    int num = GetSize() / 2;
    recipient->CopyNFrom(serieskeys, num);
    for (int i = 0; i < GetSize() / 2; i++) {
        serieskeys[i] = serieskeys[i + GetSize() / 2];
    }
    IncreaseSize(-1 * num);
    recipient->IncreaseSize(num);


}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */

void BPlusTreeLeafEntry::CopyNFrom(SeriesKey* items, int size) {
    for (int i = 0; i < size; i++) {
        Insert(items[i]);
    }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
 /*
  * For the given key, check to see whether it exists in the leaf entry. If it
  * does, then store its corresponding value in input "value" and return true.
  * If the key does not exist, then return false
  */

bool BPlusTreeLeafEntry::Lookup(const SeriesKey& key) const {
    int l = 0;
    int r = GetSize();
    int mid;
    while (l < r) {
        mid = (l + r) / 2;
        if (key == serieskeys[mid]) {
            return true;
        }
        else if (key < serieskeys[mid]) {
            l = mid + 1;
        }
        else {
            r = mid;
        }
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /*
  * First look through leaf entry to see whether delete key exist or not. If
  * exist, perform deletion, otherwise return immediately.
  * NOTE: store key&value pair continuously after deletion
  * @return   entry size after deletion
  */

int BPlusTreeLeafEntry::RemoveAndDeleteRecord(const SeriesKey& key) {
    int pos = KeyIndex(key);
    if (pos < GetSize() && (serieskeys[pos] == key)) {
        for (int j = pos + 1; j < GetSize(); j++) {
            serieskeys[j - 1] = serieskeys[j];
        }
        IncreaseSize(-1);
    }
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
 /*
  * Remove all of key & value pairs from this entry to "recipient" entry. Don't forget
  * to update the next_entry id in the sibling entry
  */

void BPlusTreeLeafEntry::MoveAllTo(BPlusTreeLeafEntry* recipient) {
    for (int i = 0; i < GetSize(); i++) {
        recipient->CopyLastFrom(*(serieskeys + i));
    }
    IncreaseSize(-1 * GetSize());
    recipient->SetNextEntryId(GetNextEntryId());
    SetNextEntryId(INVALID_ENTRY_ID);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
 /*
  * Remove the first key & value pair from this entry to "recipient" entry.
  */

void BPlusTreeLeafEntry::MoveFirstToEndOf(BPlusTreeLeafEntry* recipient, SeriesIndex* seriesindex_entrys) {
    SeriesKey first = GetItem(0);
    recipient->CopyLastFrom(first);

    for (int i = 0; i < GetSize(); i++) {
        serieskeys[i] = serieskeys[i + 1];
    }
    IncreaseSize(-1);

    entry_id_t parentId = GetParentEntryId();
    SeriesIndex* entry = &seriesindex_entrys[parentId];
    BPlusTreeInternalEntry* parent_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
    parent_entry->SetKeyAt(parent_entry->ValueIndex(GetEntryId()), GetItem(0));

}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */

void BPlusTreeLeafEntry::CopyLastFrom(const SeriesKey& item) {
    serieskeys[GetSize()] = item;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this entry to "recipient" entry.
 */

void BPlusTreeLeafEntry::MoveLastToFrontOf(BPlusTreeLeafEntry* recipient, SeriesIndex* seriesindex_entrys) {
    SeriesKey last = GetItem(GetSize());
    recipient->CopyFirstFrom(last, seriesindex_entrys);
    IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */

void BPlusTreeLeafEntry::CopyFirstFrom(const SeriesKey& item, SeriesIndex* seriesindex_entrys) {
    for (int i = GetSize(); i > 0; i-- ) {
        serieskeys[i] = serieskeys[i - 1];
    }
    serieskeys[0] = item;
    IncreaseSize(1);

    SeriesIndex* entry = &seriesindex_entrys[GetParentEntryId()];
    BPlusTreeInternalEntry* parent_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
    parent_entry->SetKeyAt(parent_entry->ValueIndex(GetEntryId()), item);

}