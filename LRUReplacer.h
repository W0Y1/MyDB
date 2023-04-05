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

	//�������:frame_id �Ƴ�Ԫ�ص�frame_id
	//��������ĩβԪ�ز���Map���Ƴ�
	bool Victim(frame_id_t* frame_id) ;

	//���õ�frame_id��Ӧ��serieskeyʱ,��replacer�Ƴ�frame_id��Ӧ��Ԫ��
	void Pin(frame_id_t frame_id) ;

	//������frame_id��Ӧ��serieskeyʱ,��frame_id��Ӧ��Ԫ�ط�������ͷ��
	void Unpin(frame_id_t frame_id) ;

	//����lruMap�Ĵ�С
	size_t Size();
private:
	list<frame_id_t> lru_list;
	unordered_map<frame_id_t, list<frame_id_t>::iterator> lruMap;
	mutex latch_;
};