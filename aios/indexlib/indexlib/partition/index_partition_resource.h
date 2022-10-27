#ifndef __INDEXLIB_GLOBAL_RESOURCE_H
#define __INDEXLIB_GLOBAL_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include <kmonitor/client/MetricsReporter.h>

DECLARE_REFERENCE_CLASS(util, MemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, TaskScheduler);
DECLARE_REFERENCE_CLASS(partition, MemoryStatReporter);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCache);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
IE_NAMESPACE_END(misc);

IE_NAMESPACE_BEGIN(partition);

typedef int32_t RangeType;
struct PartitionId {
public:
    PartitionId(const std::string& tableName, RangeType from, RangeType to);
    ~PartitionId() {};
    std::string tableName;
    RangeType from;
    RangeType to;
};

class IndexPartitionResource;
DEFINE_SHARED_PTR(IndexPartitionResource);

class IndexPartitionResource {
public:
    IndexPartitionResource() = default;
    IndexPartitionResource(const IndexPartitionResource& other) = default;
    IndexPartitionResource(
            const util::MemoryQuotaControllerPtr memoryQuotaController_,
            const util::TaskSchedulerPtr taskScheduler_,
            misc::MetricProviderPtr metricProvider_,
            file_system::FileBlockCachePtr fileBlockCache_ = file_system::FileBlockCachePtr(),
            const util::SearchCachePartitionWrapperPtr searchCache_ = util::SearchCachePartitionWrapperPtr(),
            const std::string indexPluginPath_ = "")
        : memoryQuotaController(memoryQuotaController_)
        , taskScheduler(taskScheduler_)
        , metricProvider(metricProvider_)
        , fileBlockCache(fileBlockCache_)
        , searchCache(searchCache_)
        , indexPluginPath(indexPluginPath_)
        , checkSecondIndexIntervalInMin(-1)
        , subscribeSecondIndexIntervalInMin(-1)
    {}

public:
    // for global resource
    static IndexPartitionResourcePtr Create(
            int64_t totalQuota, misc::MetricProviderPtr metricProvider,
            const char* fileBlockCacheParam, const char* searchCacheParam);
    static IndexPartitionResourcePtr Create(
            int64_t totalQuota, const kmonitor::MetricsReporterPtr &reporter,
            const char* fileBlockCacheParam, const char* searchCacheParam);

    // for partition resource
    static IndexPartitionResource Create(
            const IndexPartitionResourcePtr& globalResource,
            misc::MetricProviderPtr metricProvider,
            uint64_t pidHash);
    static IndexPartitionResource Create(
            const IndexPartitionResourcePtr& globalResource,
            const kmonitor::MetricsReporterPtr &reporter,
            uint64_t pidHash);

private:
    typedef std::vector<std::pair<std::string, std::string>> KVVec;
    static KVVec getTags(const PartitionId &pid);
public:
    util::MemoryQuotaControllerPtr memoryQuotaController;
    util::MemoryQuotaControllerPtr realtimeQuotaController;
    util::TaskSchedulerPtr taskScheduler;
    misc::MetricProviderPtr metricProvider = nullptr;
    file_system::FileBlockCachePtr fileBlockCache;
    util::SearchCachePartitionWrapperPtr searchCache;
    MemoryStatReporterPtr memStatReporter;
    std::string indexPluginPath;
    std::string partitionGroupName;
    index_base::ParallelBuildInfo parallelBuildInfo;
    int64_t checkSecondIndexIntervalInMin = -1;
    int64_t subscribeSecondIndexIntervalInMin = -1;
    PartitionRange range;
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_GLOBAL_RESOURCE_H
