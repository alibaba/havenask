#include "indexlib/file_system/file_block_cache_task_item.h"
#include "indexlib/util/cache/block_cache.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileBlockCacheTaskItem);

FileBlockCacheTaskItem::FileBlockCacheTaskItem(
        util::BlockCache* blockCache,
        misc::MetricProviderPtr metricProvider)
    : mBlockCache(blockCache)
    , mMetricProvider(metricProvider)
{
    DeclareMetrics();
}

void FileBlockCacheTaskItem::Run()
{
    ReportMetrics();
}

void FileBlockCacheTaskItem::DeclareMetrics()
{
    IE_INIT_METRIC_GROUP(mMetricProvider, BlockCacheMemUse,
                         "global/BlockCacheMemUse", kmonitor::STATUS, "byte");
}

void FileBlockCacheTaskItem::ReportMetrics()
{
    IE_REPORT_METRIC(BlockCacheMemUse, mBlockCache->GetMemoryUse());
    mBlockCache->ReportPercentile();
    mBlockCache->ReportMetrics();
}

IE_NAMESPACE_END(file_system);

