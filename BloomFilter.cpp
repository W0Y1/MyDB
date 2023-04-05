//#include "BloomFilter(´æÔÚbug).h"
//
//
//BloomFilter::BloomFilter(uint64_t size, size_t hashcount)
//    :_data(size* hashcount), _hash_count(hashcount)
//{
//
//}
//
//BloomFilter::~BloomFilter(){
//
//}
//
//void BloomFilter::add(const char* str){
//    uint64_t num;
//
//    switch (_hash_count){
//    case 8:
//        num = hash(str) % _data.size();
//        _data.set_value(num, true);
//    case 7:
//        num = SDBMHash(str) % _data.size();
//        _data.set_value(num, true);
//    case 6:
//        num = RSHash(str) % _data.size();
//        _data.set_value(num, true);
//    case 5:
//        num = JSHash(str) % _data.size();
//        _data.set_value(num, true);
//    case 4:
//        num = PJWHash(str) % _data.size();
//        _data.set_value(num, true);
//    case 3:
//        num = BKDRHash(str) % _data.size();
//        _data.set_value(num, true);
//    case 2:
//        num = DJBHash(str) % _data.size();
//        _data.set_value(num, true);
//    case 1:
//        num = APHash(str) % _data.size();
//        _data.set_value(num, true);
//    }
//}
//
//bool BloomFilter::find(const char* str){
//    uint64_t num;
//
//    switch (_hash_count){
//    case 8:
//        num = hash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 7:
//        num = SDBMHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 6:
//        num = RSHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 5:
//        num = JSHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 4:
//        num = PJWHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 3:
//        num = BKDRHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 2:
//        num = DJBHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    case 1:
//        num = APHash(str) % _data.size();
//        if (!_data[num])
//            return false;
//    }
//
//
//    return true;
//}
//
//uint64_t BloomFilter::SDBMHash(const char* str){
//    uint64_t hash = 0;
//
//    while (*str){
//        hash = (*str++) + (hash << 6) + (hash << 16) - hash;
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::RSHash(const char* str){
//    uint64_t b = 378551;
//    uint64_t a = 63689;
//    uint64_t hash = 0;
//
//    while (*str){
//        hash = hash * a + (*str++);
//        a *= b;
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::JSHash(const char* str){
//    uint64_t hash = 1315423911;
//
//    while (*str){
//        hash ^= ((hash << 5) + (*str++) + (hash >> 2));
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::PJWHash(const char* str){
//    uint64_t b = (uint64_t)(sizeof(uint64_t) * 8);
//    uint64_t t = (uint64_t)((b * 3) / 4);
//    uint64_t o = (uint64_t)(b / 8);
//    uint64_t h = (uint64_t)(0xFFFFFFFF) << (b - o);
//    uint64_t hash = 0;
//    uint64_t test = 0;
//
//    while (*str){
//        hash = (hash << o) + (*str++);
//        if ((test = hash & h) != 0){
//            hash = ((hash ^ (test >> t)) & (~h));
//        }
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::BKDRHash(const char* str){
//    uint64_t seed = 13131313131;
//    uint64_t hash = 0;
//
//    while (*str){
//        hash = hash * seed + (*str++);
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::DJBHash(const char* str){
//    uint64_t hash = 5381;
//
//    while (*str){
//        hash += (hash << 5) + (*str++);
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::APHash(const char* str){
//    uint64_t hash = 0;
//    int i;
//
//    for (i = 0; *str; i++){
//        if ((i & 1) == 0){
//            hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
//        }
//        else{
//            hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
//        }
//    }
//
//    return (hash & 0x7FFFFFFF);
//}
//
//uint64_t BloomFilter::hash(const char* str){
//    uint64_t h = 0;
//
//    while (*str){
//        h = 31 * h + (*str++);
//    }
//
//    return h % 29989;
//}
