#ifndef __INDEXLIB_FILE_BLOCK_CACHE_TASK_ITEM_H
#define __INDEXLIB_FILE_BLOCK_CACHE_TASK_ITEM_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/task_item.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(util, BlockCache);

IE_NAMESPACE_BEGIN(file_system);

class FileBlockCacheTaskItem : public util::TaskItem
{
public:
    FileBlockCacheTaskItem(util::BlockCache* blockCache,
                           misc::MetricProviderPtr metricProvider);
    ~FileBlockCacheTaskItem() = default;

public:
    void Run() override;

private:
    void DeclareMetrics();
    void ReportMetrics();

private:
    util::BlockCache* mBlockCache;
    misc::MetricProviderPtr mMetricProvider;

    IE_DECLARE_METRIC(BlockCacheMemUse);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileBlockCacheTaskItem);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_BLOCK_CACHE_TASK_ITEM_H
