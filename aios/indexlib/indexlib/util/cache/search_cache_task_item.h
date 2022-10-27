#ifndef __INDEXLIB_SEARCH_CACHE_TASK_ITEM_H
#define __INDEXLIB_SEARCH_CACHE_TASK_ITEM_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/task_item.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(util, SearchCache);

IE_NAMESPACE_BEGIN(util);

class SearchCacheTaskItem : public TaskItem
{
public:
    SearchCacheTaskItem(util::SearchCache* searchCache,
                        misc::MetricProviderPtr metricProvider);
    ~SearchCacheTaskItem();
public:
    void Run() override;

private:
    void DeclareMetrics();
    void ReportMetrics();

private:
    util::SearchCache* mSearchCache;
    misc::MetricProviderPtr mMetricProvider;

    IE_DECLARE_METRIC(SearchCacheMemUse);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchCacheTaskItem);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SEARCH_CACHE_TASK_ITEM_H
