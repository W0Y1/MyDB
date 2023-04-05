#pragma once
#include<list>
#include<unordered_map>
#include<mutex>
#include "config.h"
using namespace std;

class LRUReplacer {
public:
	LRUReplacer(size_t num_entrys);

	~LRUReplacer() ;

	//输出参数:frame_id 移除元素的frame_id
	//弹出链表末尾元素并冲Map中移除
	bool Victim(frame_id_t* frame_id) ;

	//当用到frame_id对应的serieskey时,从replacer移除frame_id对应的元素
	void Pin(frame_id_t frame_id) ;

	//当用完frame_id对应的serieskey时,将frame_id对应的元素放置链表头部
	void Unpin(frame_id_t frame_id) ;

	//返回lruMap的大小
	size_t Size();
private:
	list<frame_id_t> lru_list;
	unordered_map<frame_id_t, list<frame_id_t>::iterator> lruMap;
	mutex latch_;
};