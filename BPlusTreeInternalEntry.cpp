#include"BPlusTreeInternalEntry.h"

void BPlusTreeInternalEntry::Init(entry_id_t entry_id, entry_id_t parent_id, int max_size) {
    SetEntryId(entry_id);
    SetParentEntryId(parent_id);
    SetMaxSize(max_size);
    SetEntryType(INTERNAL_ENTRY);
    SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */


SeriesKey BPlusTreeInternalEntry::KeyAt(int index) const {
    // replace with your own code
    return array[index].first;
}


void BPlusTreeInternalEntry::SetKeyAt(int index, const SeriesKey& key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */

int BPlusTreeInternalEntry::ValueIndex(const entry_id_t& value) const {
    for (int i = 0; i <= GetSize(); i++) {
        if (array[i].second == value) {
            return i;
        }
    }
    return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */

entry_id_t BPlusTreeInternalEntry::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
 /*
  * Find and return the child pointer(entry_id) which points to the child entry
  * that contains input "key"
  * Start the search from the second key(the first key should always be invalid)
  */

entry_id_t BPlusTreeInternalEntry::Lookup(const SeriesKey& key) const {
    for (int i = 1; i < GetSize(); i++) {
        SeriesKey cur_key = array[i].first;
        if (key < cur_key) {
            return array[i - 1].second;
        }
    }
    return array[GetSize() - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /*
  * Populate new root entry with old_value + new_key & new_value
  * When the insertion cause overflow from leaf entry all the way upto the root
  * entry, you should create a new root entry and populate its elements.
  * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
  */

void BPlusTreeInternalEntry::PopulateNewRoot(const entry_id_t& old_value, const SeriesKey& new_key,
    const entry_id_t& new_value) {
    array[0].second = old_value;
    array[1] = pair<SeriesKey, entry_id_t>{ new_key, new_value };
    IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */

int BPlusTreeInternalEntry::InsertNodeAfter(const entry_id_t& old_value, const SeriesKey& new_key,
    const entry_id_t& new_value) {
    int index = ValueIndex(old_value);
    for (int i = GetSize(); i > index; i--) {
        array[i + 1] = array[i];
    }
    array[index + 1] = pair<SeriesKey, entry_id_t>{ new_key, new_value };
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
 /*
  * Remove half of key & value pairs from this entry to "recipient" entry
  */

void BPlusTreeInternalEntry::MoveHalfTo(BPlusTreeInternalEntry* recipient,SeriesIndex* seriesindex_entrys) {
    int num = GetSize() / 2;
    recipient->CopyNFrom(array, num);
    for (int i = 0; i < GetSize() / 2; i++) {
        array[i] = array[i + GetSize() / 2];
    }
    IncreaseSize(-1 * num);
    recipient->IncreaseSize(num);


    for (int i = 0; i < recipient->GetSize(); i++) {
        entry_id_t entry_id = recipient->ValueAt(i);
        auto entry = &seriesindex_entrys[entry_id];
        SeriesIndex* bp = reinterpret_cast<SeriesIndex*>(entry);
        bp->SetParentEntryId(recipient->GetEntryId());
    }

}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal entry, for all entries (entrys) moved, their parents entry now changes to me.
 * So I need to 'adopt' them by changing their parent entry id, which needs to be persisted with BufferPoolManger
 */

void BPlusTreeInternalEntry::CopyNFrom(pair<SeriesKey, entry_id_t>* items, int size) {
    for (int i = 0; i < size; i++) {
        InsertNodeAfter(array[GetSize() - 1].second, items[i].first, items[i].second);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /*
  * Remove the key & value pair in internal entry according to input index(a.k.a
  * array offset)
  * NOTE: store key&value pair continuously after deletion
  */

void BPlusTreeInternalEntry::Remove(int index) {
    for (int i = index; i < GetSize() - 1; i++) {
        array[i] = array[i + 1];
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal entry and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */

entry_id_t BPlusTreeInternalEntry::RemoveAndReturnOnlyChild() { return INVALID_ENTRY_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
 /*
  * Remove all of key & value pairs from this entry to "recipient" entry.
  * The middle_key is the separation key you should get from the parent. You need
  * to make sure the middle key is added to the recipient to maintain the invariant.
  * You also need to use BufferPoolManager to persist changes to the parent entry id for those
  * entrys that are moved to the recipient
  */

void BPlusTreeInternalEntry::MoveAllTo(BPlusTreeInternalEntry* recipient, const int& middle_key, SeriesIndex* seriesindex_entrys) {
    SeriesIndex* entry = &seriesindex_entrys[GetParentEntryId()];
    BPlusTreeInternalEntry* parent_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
    array[0].first = parent_entry->KeyAt(middle_key);

    recipient->CopyNFrom(array, GetSize());

    for (int i = 0; i < GetSize(); i++) {
        entry_id_t child_entry_id = ValueAt(i);
        entry = &seriesindex_entrys[child_entry_id];
        BPlusTreeInternalEntry* child_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);

        child_entry->SetParentEntryId(recipient->GetEntryId());
    }
    IncreaseSize(-1 * GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
 /*
  * Remove the first key & value pair from this entry to tail of "recipient" entry.
  *
  * The middle_key is the separation key you should get from the parent. You need
  * to make sure the middle key is added to the recipient to maintain the invariant.
  * You also need to use BufferPoolManager to persist changes to the parent entry id for those
  * entrys that are moved to the recipient
  */

void BPlusTreeInternalEntry::MoveFirstToEndOf(BPlusTreeInternalEntry* recipient, const int& middle_key, SeriesIndex* seriesindex_entrys) {
    pair<SeriesKey, entry_id_t> pair{ KeyAt(1), ValueAt(0) };
    entry_id_t child_entry_id = ValueAt(0);
    array[0].second = ValueAt(1);
    Remove(1);

    recipient->CopyLastFrom(pair, seriesindex_entrys);

    auto* entry = &seriesindex_entrys[child_entry_id];

    auto child = reinterpret_cast<SeriesIndex*>(entry);
    child->SetParentEntryId(recipient->GetEntryId());
}


/* Append an entry at the end.
 * Since it is an internal entry, the moved entry(entry)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent entry id, which needs to be persisted with BufferPoolManger
 */

void BPlusTreeInternalEntry::CopyLastFrom(const pair<SeriesKey, entry_id_t>& pair, SeriesIndex* seriesindex_entrys) {
    entry_id_t entryId = pair.second;
    SeriesIndex* entry = &seriesindex_entrys[entryId];
    auto* parent = reinterpret_cast<BPlusTreeInternalEntry*>(entry);

    auto index = parent->ValueIndex(GetEntryId());
    auto key = parent->KeyAt(index + 1);

    array[GetSize()] = { key, pair.second };
    IncreaseSize(1);
    parent->SetKeyAt(index + 1, pair.first);

    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this entry to head of "recipient" entry.
 * You need to handle the original dummy key properly, e.g. updating recipient¡¯s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent entry id for those entrys that are
 * moved to the recipient
 */

void BPlusTreeInternalEntry::MoveLastToFrontOf(BPlusTreeInternalEntry* recipient, const int& middle_key, SeriesIndex* seriesindex_entrys) {
    pair<SeriesKey, entry_id_t> pair = array[GetSize() - 1];
    recipient->CopyFirstFrom(pair, seriesindex_entrys);
    IncreaseSize(-1);

    SeriesIndex* entry = &seriesindex_entrys[GetParentEntryId()];
    auto* parent = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
    parent->SetKeyAt(middle_key, pair.first);

    SeriesIndex* child_entry = &seriesindex_entrys[pair.second];
    auto* child = reinterpret_cast<BPlusTreeInternalEntry*>(child_entry);
    child->SetParentEntryId(recipient->GetEntryId());

}

/* Append an entry at the beginning.
 * Since it is an internal entry, the moved entry(entry)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent entry id, which needs to be persisted with BufferPoolManger
 */

void BPlusTreeInternalEntry::CopyFirstFrom(const pair<SeriesKey, entry_id_t>& pair, SeriesIndex* seriesindex_entrys) {
    for (int i = GetSize() - 1; i >= 0; i--) {
        array[i + 1] = array[i];
    }
    SeriesIndex* entry = &seriesindex_entrys[GetParentEntryId()];
    auto* parent = reinterpret_cast<BPlusTreeInternalEntry*>(entry);

    array[0].second = pair.second;
    array[1].first = parent->KeyAt(1);

    IncreaseSize(1);
}