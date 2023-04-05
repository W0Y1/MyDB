#include "BPlusTree.h"

BPlusTree::BPlusTree(entry_id_t root_entry_id, SeriesIndex* seriesindex, int entry_num, int leaf_max_size, int internal_max_size)
    :root_entry_id_(root_entry_id),
    seriesindex_entrys_(seriesindex),
    series_index_num(entry_num),
    leaf_max_size_(leaf_max_size),
    internal_max_size_(internal_max_size) {}


bool BPlusTree::IsEmpty() const {
    if (root_entry_id_ == INVALID_ENTRY_ID) {
        return true;
    }
    return false;

}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
 /*
  * Return the only value that associated with input key
  * This method is used for point query
  * @return : true means key exists
  */
bool BPlusTree::GetValue(const SeriesKey& key, std::vector<SeriesKey>* result) {
    SeriesIndex* entry = FindLeafEntry(key, false);
    BPlusTreeLeafEntry* leafEntry = reinterpret_cast<BPlusTreeLeafEntry*>(entry);

    int index = leafEntry->KeyIndex(key);
    if (key != leafEntry->KeyAt(index)) {
        return false;
    }
    SeriesKey item = leafEntry->GetItem(index);
    result->push_back(item);
    return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /*
  * Insert constant key & value pair into b+ tree
  * if current tree is empty, start new tree, update root page id and insert
  * entry, otherwise insert into leaf page.
  * @return: since we only support unique key, if user try to insert duplicate
  * keys return false, otherwise return true.
  */
bool BPlusTree::Insert(const SeriesKey& key) {
    if (IsEmpty()) {
        StartNewTree(key);
        return true;
    }
    return InsertIntoLeaf(key);

}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(const SeriesKey& key) {
    SeriesIndex* newEntry = new SeriesIndex;
    root_entry_id_ = 0;
    series_index_num++;
    newEntry = &seriesindex_entrys_[root_entry_id_];
    //��ȡд��

    BPlusTreeLeafEntry* leaf_entry = reinterpret_cast<BPlusTreeLeafEntry*>(newEntry);
    leaf_entry->Init(root_entry_id_, INVALID_ENTRY_ID, leaf_max_size_);

    leaf_entry->Insert(key);
    //�ͷ�+UnPin
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(const SeriesKey& key) {
    SeriesIndex* entry = FindLeafEntry(key, false);
    BPlusTreeLeafEntry* leaf_entry = reinterpret_cast<BPlusTreeLeafEntry*>(entry);
    //�Ե�ǰleaf_entry��ȡд�������ж��Ƿ�ȫ�ͷ�֮ǰ����
    if (leaf_entry->Lookup(key)){
        //�ͷ�+Unpin
            return false;
    }
    leaf_entry->Insert(key);

    if (leaf_entry->GetSize() == leaf_entry->GetMaxSize() + 1) {
        BPlusTreeLeafEntry* new_LeafEntry = Split(leaf_entry);
        //��ȡnew_LeafEntry��д��
        InsertIntoParent(leaf_entry, new_LeafEntry->KeyAt(0), new_LeafEntry);
        //�ͷ�new_LeafEntry��д��
    }
    //�ͷ�+Unpin
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
template <typename N>
N* BPlusTree::Split(N* node) {
    entry_id_t new_entry_id = series_index_num;
    series_index_num++;
    SeriesIndex* new_entry = (SeriesIndex*)malloc(4096);
    new_entry = &seriesindex_entrys_[new_entry_id];
    //��new_entry��ȡд��
    if (new_entry == nullptr) {
        //�ͷ�new_entry��
        throw "out of memory";
    }
    N* new_node;
    if (node->IsLeafEntry()) {
        BPlusTreeLeafEntry* leaf_node = reinterpret_cast<BPlusTreeLeafEntry*>(node);
        BPlusTreeLeafEntry* new_leaf_node = reinterpret_cast<BPlusTreeLeafEntry*>(new_entry);
        new_leaf_node->Init(new_entry_id, leaf_node->GetParentEntryId(), leaf_max_size_);
        leaf_node->MoveHalfTo(new_leaf_node);

        new_leaf_node->SetNextEntryId(leaf_node->GetNextEntryId());
        leaf_node->SetNextEntryId(new_leaf_node->GetEntryId());
        new_node = reinterpret_cast<N*>(new_leaf_node);
    }
    else {
        BPlusTreeInternalEntry* internal_node = reinterpret_cast<BPlusTreeInternalEntry*>(node);
        BPlusTreeInternalEntry* new_internal_node = reinterpret_cast<BPlusTreeInternalEntry*>(new_entry);
        new_internal_node->Init(new_entry_id, internal_node->GetParentEntryId(), internal_max_size_);
        internal_node->MoveHalfTo(new_internal_node, seriesindex_entrys_);
        new_node = reinterpret_cast<N*>(new_internal_node);
    }
    //�ͷ�new_entry��
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(SeriesIndex* old_node, const SeriesKey& key, SeriesIndex* new_node) {
    if (old_node->IsRootEntry()) {
        entry_id_t  new_root_pid = series_index_num;
        series_index_num++;
        SeriesIndex* new_entry = (SeriesIndex*)malloc(4096);
        new_entry = &seriesindex_entrys_[new_root_pid];
        BPlusTreeInternalEntry* new_root_entry = reinterpret_cast<BPlusTreeInternalEntry*>(new_entry);
        //��ȡnew_entryд��

        new_root_entry->Init(new_root_pid, INVALID_ENTRY_ID, internal_max_size_);

        root_entry_id_ = new_root_pid;

        new_root_entry->PopulateNewRoot(old_node->GetEntryId(), key, new_node->GetEntryId());
        old_node->SetParentEntryId(new_root_pid);
        new_node->SetParentEntryId(new_root_pid);
        //�ͷ�new_entry+UnPin
    }
    else {
        entry_id_t parent_id = old_node->GetParentEntryId();
        SeriesIndex* parent_entry = &seriesindex_entrys_[parent_id];
        BPlusTreeInternalEntry* parent_node = reinterpret_cast<BPlusTreeInternalEntry*>(parent_entry);
        //��ȡparent_entryд��
        parent_node->InsertNodeAfter(old_node->GetEntryId(), key, new_node->GetEntryId());
        if (parent_node->GetSize() == parent_node->GetMaxSize() + 1) {
            BPlusTreeInternalEntry* new_parent_node = Split(parent_node);
            //��ȡnew_parent_node��д��
            InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node);
            //�ͷ�new_parent_entry��parent_entry+UnPin
        }
        //�ͷ�parent+UnPin
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /*
  * Delete key & value pair associated with input key
  * If current tree is empty, return immdiately.
  * If not, User needs to first find the right leaf page as deletion target, then
  * delete entry from leaf page. Remember to deal with redistribute or merge if
  * necessary.
  */
void BPlusTree::Remove(const SeriesKey& key) {
    if (IsEmpty()) {
        return;
    }
    SeriesIndex* entry = FindLeafEntry(key, false);
    //��ȡentryд�������ݲ����Ƿ�ȫ�ͷ�֮ǰ����
    if (entry == nullptr) {
        return;
    }
    BPlusTreeLeafEntry* leafEntry = reinterpret_cast<BPlusTreeLeafEntry*>(entry);

    int size = leafEntry->RemoveAndDeleteRecord(key);
    if (size < leafEntry->GetMinSize()) {
        CoalesceOrRedistribute(leafEntry);
    }
    else {
        //�ͷ�+Unpin
    }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N* node) {
    if (node->IsRootEntry()) {
        return AdjustRoot(node);
    }

    N* sibling = nullptr;
    bool isLeftSibling = FindSibling(node, sibling);

    SeriesIndex* entry = &seriesindex_entrys_[node->GetParentEntryId()];
    BPlusTreeInternalEntry* parent_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
    //��ȡparent_entry��д��
    int nodeIndexInParent = parent_entry->ValueIndex(node->GetEntryId());

    if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {
        // ��ȡsibling��д��
        Coalesce(sibling, node, parent_entry, nodeIndexInParent);
        //�ͷ�sibling��д��
        return true;
    }
    else {
        // ��ȡsibling��д��
        Redistribute(sibling, node, nodeIndexInParent);
        //�ͷ�sibling��д��
    }
    //�ͷ�parent_entry
    return false;
}

/*
 *  �������true����ʾ�ҵ�node�����ֵܽڵ㣬false��ʾ�ҵ�node�Ҳ���ֵܽڵ�
 */
template <typename N>
bool BPlusTree::FindSibling(N* node, N*& sibling) {
    SeriesIndex* entry = &seriesindex_entrys_[node->GetParentEntryId()];
    BPlusTreeInternalEntry* parent_entry = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
    //��ȡparent_entry��д��
    int index = parent_entry->ValueIndex(node->GetEntryId());

    int siblingIndex;
    bool isLeftSibling;
    if (index == 0) {
        siblingIndex = index + 1;
        isLeftSibling = false;
    }
    else {
        siblingIndex = index - 1;
        isLeftSibling = true;
    }
    SeriesIndex* sibling_entry = &seriesindex_entrys_[parent_entry->ValueAt(siblingIndex)];
    sibling = reinterpret_cast<N*>(sibling_entry);
    //�ͷ�parent_entry
    return isLeftSibling;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
template <typename N>
bool BPlusTree::Coalesce(N* neighbor_node, N* node,BPlusTreeInternalEntry* parent, int index) {
    entry_id_t entryid = neighbor_node->GetEntryId();
    if (index > 0) {
        if (!node->IsLeafEntry()) {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeInternalEntry* node = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeInternalEntry* neighbor_node = reinterpret_cast<BPlusTreeInternalEntry*>(entry_neighbor_node);
            node->BPlusTreeInternalEntry::MoveAllTo(neighbor_node, index, seriesindex_entrys_);
        }
        else {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeLeafEntry* node = reinterpret_cast<BPlusTreeLeafEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeLeafEntry* neighbor_node = reinterpret_cast<BPlusTreeLeafEntry*>(entry_neighbor_node);
            node->MoveAllTo(neighbor_node);
        }
        parent->Remove(index);
    }
    else {
        if (!neighbor_node->IsLeafEntry()) {
            SeriesIndex* entry = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeInternalEntry* neighbor_node = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
            SeriesIndex* entry_node = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeInternalEntry* node = reinterpret_cast<BPlusTreeInternalEntry*>(entry_node);
            neighbor_node->BPlusTreeInternalEntry::MoveAllTo(node, index, seriesindex_entrys_);
        }
        else {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeLeafEntry* node = reinterpret_cast<BPlusTreeLeafEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeLeafEntry* neighbor_node = reinterpret_cast<BPlusTreeLeafEntry*>(entry_neighbor_node);
            node->MoveAllTo(neighbor_node);
        }
        parent->Remove(index + 1);
    }

    if (parent->GetSize() < parent->GetMinSize()) {
        return CoalesceOrRedistribute(parent);
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
template <typename N>
void BPlusTree::Redistribute(N* neighbor_node, N* node, int index) {
    if (node->IsLeafEntry()) {
        if (index == 0) {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeLeafEntry* node = reinterpret_cast<BPlusTreeLeafEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeLeafEntry* neighbor_node = reinterpret_cast<BPlusTreeLeafEntry*>(entry_neighbor_node);
            neighbor_node->MoveFirstToEndOf(node, seriesindex_entrys_);
        }
        else {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeLeafEntry* node = reinterpret_cast<BPlusTreeLeafEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeLeafEntry* neighbor_node = reinterpret_cast<BPlusTreeLeafEntry*>(entry_neighbor_node);
            neighbor_node->MoveLastToFrontOf(node, seriesindex_entrys_);
        }
    }
    else {
        if (index == 0) {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeInternalEntry* node = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeInternalEntry* neighbor_node = reinterpret_cast<BPlusTreeInternalEntry*>(entry_neighbor_node);
            neighbor_node->MoveFirstToEndOf(node, index, seriesindex_entrys_);
        }
        else {
            SeriesIndex* entry = &seriesindex_entrys_[node->GetEntryId()];
            BPlusTreeInternalEntry* node = reinterpret_cast<BPlusTreeInternalEntry*>(entry);
            SeriesIndex* entry_neighbor_node = &seriesindex_entrys_[neighbor_node->GetEntryId()];
            BPlusTreeInternalEntry* neighbor_node = reinterpret_cast<BPlusTreeInternalEntry*>(entry_neighbor_node);
            neighbor_node->MoveLastToFrontOf(node, index, seriesindex_entrys_);
        }
    }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
bool BPlusTree::AdjustRoot(SeriesIndex* old_root_node) {
    if (old_root_node->IsLeafEntry() && old_root_node->GetSize() == 0) {
        root_entry_id_ = INVALID_ENTRY_ID;
        return true;
    }
    if (!old_root_node->IsLeafEntry() && old_root_node->GetSize() == 1) {
        BPlusTreeInternalEntry* old_root = reinterpret_cast<BPlusTreeInternalEntry*>(old_root_node);
        root_entry_id_ = old_root->ValueAt(0);

        SeriesIndex* new_root_entry = &seriesindex_entrys_[root_entry_id_];
        SeriesIndex* new_root = reinterpret_cast<SeriesIndex*>(new_root_entry);
        //��ȡroot_entry_id_��д��
        new_root->SetParentEntryId(INVALID_ENTRY_ID);
        //�ͷ�+Unpin
        return true;
    }
    return false;
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
SeriesIndex* BPlusTree::FindLeafEntry(const SeriesKey& key, bool leftMost) {
    SeriesIndex* entry = &seriesindex_entrys_[root_entry_id_];
    SeriesIndex* node = reinterpret_cast<SeriesIndex*>(entry);

    while (!node->IsLeafEntry()) {
        //�жϲ�����entryд�����߶���
        //������жϵ�ǰ�ڵ��Ƿ�ȫ����ȡtransaction �ͷ�֮ǰ�ڵ�entry����
        BPlusTreeInternalEntry* internal_node = reinterpret_cast<BPlusTreeInternalEntry*>(node);
        entry_id_t next_entry_id;

        if (leftMost == true) {
            next_entry_id = internal_node->ValueAt(0);
        }
        else {
            next_entry_id = internal_node->Lookup(key);
        }
        //��������entry����transaction
        SeriesIndex* next_entry = &seriesindex_entrys_[next_entry_id];
        entry = next_entry;
    }
    return entry;
}

