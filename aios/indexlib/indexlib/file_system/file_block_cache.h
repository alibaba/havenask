#ifndef __INDEXLIB_FILE_BLOCK_CACHE_H
#define __INDEXLIB_FILE_BLOCK_CACHE_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"

// declare
DECLARE_REFERENCE_CLASS(util, BlockCache);
DECLARE_REFERENCE_CLASS(util, TaskScheduler);

IE_NAMESPACE_BEGIN(util);
template<typename T> class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
DEFINE_SHARED_PTR(SimpleMemoryQuotaController);
IE_NAMESPACE_END(util);

IE_NAMESPACE_BEGIN(file_system);

struct BlockCacheMetrics;

class FileBlockCache
{
public:
    FileBlockCache();
    ~FileBlockCache();

public:
    // for global block cache
    bool Init(const std::string& configStr,
              const util::MemoryQuotaControllerPtr& globalMemoryQuotaController,
              const util::TaskSchedulerPtr& taskScheduler = util::TaskSchedulerPtr(),
              misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());

public:
    // for local block cache
    bool Init(size_t cacheSize, uint32_t blockSize,
              const util::SimpleMemoryQuotaControllerPtr& quotaController);

public:
    // common
    const util::BlockCachePtr& GetBlockCache() const { return mBlockCache; }

    // TODO: remove this
    size_t GetMemoryUse() const;

    size_t GetCacheSize() const;
    size_t GetUsage() const;

    static uint64_t GetFileId(const std::string& fileName);

    void ReportMetrics();

private:
    bool RegisterMetricsReporter(misc::MetricProviderPtr metricProvider);

private:
    static constexpr const char* _CONFIG_SEPERATOR = ";";
    static constexpr const char* _CONFIG_KV_SEPERATOR = "=";
    static constexpr const char* _CONFIG_CACHE_SIZE_NAME = "cache_size";
    static constexpr const char* _CONFIG_BLOCK_SIZE_NAME = "block_size";
    static constexpr const char* _CONFIG_NUM_SHARD_BITS = "num_shard_bits";
    static constexpr const char* _CONFIG_USE_PREFETCHER = "use_prefetcher";
    static const size_t _DEFAULT_CACHE_SIZE = 1; // 1MB
    static const size_t _DEFAULT_BLOCK_SIZE = 4 * 1024; // 4KB

private:
    util::BlockCachePtr mBlockCache;
    util::SimpleMemoryQuotaControllerPtr mCacheMemController;
    util::MemoryQuotaControllerPtr mGlobalMemoryQuotaController;
    util::TaskSchedulerPtr mTaskScheduler;
    int32_t mReportMetricsTaskId;

public:
    bool TEST_mQuickExit;

private:
    friend class BlockFileNodeCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileBlockCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_BLOCK_CACHE_H
