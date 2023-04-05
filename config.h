#pragma once
#include<stdint.h>
#include<string>

using std::string;

static constexpr int DATA_BLOCK_MAXSIZE = 16384;													//16KB
static constexpr int POOL_SIZE = 32;
static constexpr int INVALID_ENTRY_ID = -1;
static constexpr int INVALID_ID = -1;
static constexpr int SEGEMENT_SIZE = 4194304;										//4MB
static constexpr int ENTRY_SIZE = 4096;	
static constexpr int SERIIES_INDEX_BLOCK_SIZE = 4096;						
static constexpr int SERIES_PARTITION_NUM = 8;
static constexpr int CACHE_SIZE = 26214400;											//25MB
static constexpr int VIRTUAL_NODE_NUM = 32;
static constexpr int BLOOM_FILTER_SIZE = 512;
static constexpr int HASH_COUNT = 4;

static string PATH_FILE = "E:\\MyDB";

using frame_id_t = uint32_t;
using SeriesId = uint64_t;																		//seq+PartitionN
using SeriesOffset = uint64_t;																	//| SeriesSegementId(32) |+|offset in Segement(32)|
using entry_id_t = uint32_t;
