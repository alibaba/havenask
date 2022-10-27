#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/cache/search_cache.h"
#include "indexlib/util/cache/search_cache_creator.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include "indexlib/util/future_executor.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);

PartitionId::PartitionId(const string& name, RangeType f, RangeType t)
    : tableName(name)
    , from(f)
    , to(t)
{
}

IndexPartitionResourcePtr IndexPartitionResource::Create(
        int64_t totalQuota,
        const kmonitor::MetricsReporterPtr &reporter,
        const char* fileBlockCacheParam, const char* searchCacheParam)
{
    misc::MetricProviderPtr metricProvider(new misc::MetricProvider(reporter));
    return Create(totalQuota, metricProvider, fileBlockCacheParam, searchCacheParam);
}

IndexPartitionResourcePtr IndexPartitionResource::Create(
        int64_t totalQuota,
        misc::MetricProviderPtr metricProvider,
        const char* fileBlockCacheParam, const char* searchCacheParam)
{
    IndexPartitionResourcePtr resource(new IndexPartitionResource());

    resource->taskScheduler.reset(new util::TaskScheduler());
    resource->metricProvider = metricProvider;
    resource->memoryQuotaController.reset(new util::MemoryQuotaController(totalQuota));

    if (NULL != fileBlockCacheParam)
    {
        resource->fileBlockCache.reset(new file_system::FileBlockCache());
        string fileBlockCacheParamStr = fileBlockCacheParam;
        if (!resource->fileBlockCache->Init(fileBlockCacheParamStr,
                        resource->memoryQuotaController,
                        resource->taskScheduler, metricProvider))
        {
            return IndexPartitionResourcePtr();
        }
    }

    if (NULL != searchCacheParam) {
        SearchCachePtr searchCache(SearchCacheCreator::Create(searchCacheParam,
                                                              resource->memoryQuotaController,
                                                              resource->taskScheduler,
                                                              metricProvider));
        if (searchCache)
        {
            resource->searchCache.reset(new SearchCachePartitionWrapper(searchCache, 0));
        }
        else
        {
            resource->searchCache.reset();
        }
    }

    char* memStatEnv = getenv("PRINT_MALLOC_STATS");
    string memStatStr = memStatEnv ? memStatEnv : "";
    MemoryStatReporterPtr memStatReporter(new MemoryStatReporter);
    if (memStatReporter->Init(memStatStr, resource->searchCache,
                              resource->fileBlockCache, resource->taskScheduler,
                              metricProvider))
    {
        resource->memStatReporter = memStatReporter;
    }

    char* checkIndexEnv = getenv("SECONDARY_INDEX_CHECK_INTERVAL");
    if (checkIndexEnv)
    {
        int64_t value = -1;
        if (StringUtil::fromString(checkIndexEnv, value))
        {
            resource->checkSecondIndexIntervalInMin = value;
        }
    }

    char* subscribeIndexEnv = getenv("SECONDARY_INDEX_SUBSCRIBE_INTERVAL");
    if (subscribeIndexEnv)
    {
        int64_t value = -1;
        if (StringUtil::fromString(subscribeIndexEnv, value))
        {
            resource->subscribeSecondIndexIntervalInMin = value;
        }
    }
    return resource;
}

IndexPartitionResource IndexPartitionResource::Create(
        const IndexPartitionResourcePtr& globalResource,
        const kmonitor::MetricsReporterPtr &reporter,
        uint64_t partitionId)
{
    misc::MetricProviderPtr metricProvider(new misc::MetricProvider(reporter));
    return Create(globalResource, metricProvider, partitionId);
}

IndexPartitionResource IndexPartitionResource::Create(
        const IndexPartitionResourcePtr& globalResource,
        misc::MetricProviderPtr metricProvider,
        uint64_t partitionId)
{
    IndexPartitionResource partResource;
    partResource.metricProvider = metricProvider;

    if (!globalResource)
    {
        assert(false);
        return partResource;
    }
    SearchCachePartitionWrapperPtr searchCache;
    if (globalResource->searchCache)
    {
        searchCache.reset(new SearchCachePartitionWrapper(
                        globalResource->searchCache->GetSearchCache(),
                        partitionId));
    }
    partResource = *globalResource;
    partResource.searchCache = searchCache;
    partResource.metricProvider = metricProvider;
    return partResource;
}

IndexPartitionResource::KVVec IndexPartitionResource::getTags(const PartitionId &pid)
{
    IndexPartitionResource::KVVec tags;
    string partition = StringUtil::toString(pid.from);
    partition += "_" + StringUtil::toString(pid.to);
    tags.push_back(make_pair("partition", partition));
    tags.push_back(make_pair("table_name", pid.tableName));
    return tags;
}

IE_NAMESPACE_END(partition);
