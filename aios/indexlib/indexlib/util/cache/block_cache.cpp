#include "indexlib/util/cache/block_allocator.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/util/cache/lru_cache.h" // for TEST_GetRefCount
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BlockCache);

BlockCache::BlockCache() 
    : mCacheSize(0)
    , mBlockSize(0)
{
}

BlockCache::~BlockCache() 
{
    if (mCache)
    {
        mCache->EraseUnRefEntries();
    }
}

void DeleteBlock(const autil::ConstString& key, void* value,
                 const CacheAllocatorPtr& allocator)
{
    allocator->Deallocate(value);
}

bool BlockCache::Init(size_t cacheSize, uint32_t blockSize, int32_t shardBitsNum,
                      bool useClockCache, bool useprefetch)
{
    if (blockSize == 0)
    {
        IE_LOG(ERROR, "blockSize[%u] is zero", blockSize);
        return false;
    }
    
    mCacheSize = cacheSize;
    mBlockSize = blockSize;
    mUsePrefetcher = useprefetch;
    if (cacheSize < ((size_t)blockSize << shardBitsNum))
    {
        mCacheSize = blockSize << shardBitsNum;
        IE_LOG(WARN, "cacheSize[%lu] small than blockSize[%u] << %d, adjust to [%lu]",
               cacheSize, blockSize, shardBitsNum, mCacheSize);
    }

    mBlockAllocator.reset(new BlockAllocator(mCacheSize, mBlockSize));
    // not support clock cache in this version
    mCache = NewLRUCache(mCacheSize, shardBitsNum, false, 0.0, mBlockAllocator);

    return true;
}

bool BlockCache::Put(Block* block, Cache::Handle** handle)
{
    assert(mCache && block && handle);
    autil::ConstString key(reinterpret_cast<const char*>(
                    &block->id), sizeof(block->id));
    Status st = mCache->Insert(key, block, mBlockSize, &DeleteBlock, handle);
    return st.ok();
}

Block* BlockCache::Get(const blockid_t& blockId, Cache::Handle** handle)
{
    assert(mCache);
    autil::ConstString key(reinterpret_cast<const char*>(&blockId), sizeof(blockId));
    *handle = mCache->Lookup(key);
    if (*handle)
    {
        return reinterpret_cast<Block*>(mCache->Value(*handle));
    }
 
    return NULL;
}
 
void BlockCache::ReleaseHandle(Cache::Handle* handle)
{
    assert(mCache);
    if (handle)
    {
        mCache->Release(handle);
    }
} 

uint32_t BlockCache::TEST_GetRefCount(Cache::Handle* handle)
{
    if (strcmp(mCache->Name(), "LRUCache") == 0)
    {
        return reinterpret_cast<LRUHandle*>(handle)->refs;
    }
    assert(false);
    return 0;
}

void BlockCache::RegisterMetrics(misc::MetricProviderPtr metricProvider)
{
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheHitRatio,
                         "global/BlockCacheHitRatio", kmonitor::GAUGE, "%");
    IE_INIT_LOCAL_INPUT_METRIC(mHitRatioReporter, BlockCacheHitRatio);
    // HIT
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheHitQps,
                         "global/BlockCacheHitQps", kmonitor::QPS, "count");
    // MIS
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheMissQps,
                         "global/BlockCacheMissQps", kmonitor::QPS, "count"); // BlockCacheReadQps
    IE_INIT_METRIC_GROUP(metricProvider, BlockCachePrefetchQps,
                         "global/BlockCachePrefetchQps", kmonitor::QPS, "count"); // BlockCacheReadQps
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadLatency,
                         "global/BlockCacheReadLatency", kmonitor::GAUGE, "us");
    IE_INIT_LOCAL_INPUT_METRIC(mReadLatencyReporter, BlockCacheReadLatency);
    
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadCount,
                         "global/BlockCacheReadCount", kmonitor::GAUGE, "count");

    IE_INIT_METRIC_GROUP(metricProvider, LatencyPercentile90,
                         "global/LatencyPercentile90", kmonitor::GAUGE, "us");
    IE_INIT_METRIC_GROUP(metricProvider, LatencyPercentile95,
                         "global/LatencyPercentile95", kmonitor::GAUGE, "us");
    IE_INIT_METRIC_GROUP(metricProvider, LatencyPercentile99,
                         "global/LatencyPercentile99", kmonitor::GAUGE, "us");
    IE_INIT_METRIC_GROUP(metricProvider, LatencyPercentile995,
                         "global/LatencyPercentile995", kmonitor::GAUGE, "us");
    IE_INIT_METRIC_GROUP(metricProvider, LatencyPercentile999,
                         "global/LatencyPercentile999", kmonitor::GAUGE, "us");

    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadOneBlockQps,
                         "global/BlockCacheOneBlockQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadMultiBlockQps,
                         "global/BlockCacheMultiBlockQps", kmonitor::QPS, "count");


    mBlockCacheHitReporter.Init(mBlockCacheHitQpsMetric);
    mBlockCacheMissReporter.Init(mBlockCacheMissQpsMetric);
    mBlockCachePrefetchReporter.Init(mBlockCachePrefetchQpsMetric);
}

IE_NAMESPACE_END(util);

