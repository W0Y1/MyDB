#include"LRUReplacer.h"

LRUReplacer::LRUReplacer(size_t num_entrys) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t* frame_id) {
    latch_.lock();
    if (lruMap.empty()) {
        latch_.unlock();
        return false;
    }
    frame_id_t frame = lru_list.back();
    lruMap.erase(frame);

    lru_list.pop_back();
    *frame_id = frame;

    latch_.unlock();
    return true;
}


void LRUReplacer::Pin(frame_id_t frame_id) {
    latch_.lock();
    if (lruMap.count(frame_id) != 0) {
        lru_list.erase(lruMap[frame_id]);
        lruMap.erase(frame_id);
    }
    latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch_.lock();
    if (lruMap.count(frame_id) != 0) {
        latch_.unlock();
        return;
    }
    while (Size() >= POOL_SIZE) {
        frame_id_t need_del = lru_list.front();
        lru_list.pop_front();
        lruMap.erase(need_del);
    }

    lru_list.push_front(frame_id);
    lruMap[frame_id] = lru_list.begin();
    latch_.unlock();
}

size_t LRUReplacer::Size() { return lruMap.size(); }