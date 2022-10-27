#include "indexlib/util/cache/search_cache_task_item.h"
#include "indexlib/util/cache/search_cache.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SearchCacheTaskItem);

SearchCacheTaskItem::SearchCacheTaskItem(
        util::SearchCache* searchCache,
        misc::MetricProviderPtr metricProvider)
    : mSearchCache(searchCache)
    , mMetricProvider(metricProvider)
{
    DeclareMetrics();
}

SearchCacheTaskItem::~SearchCacheTaskItem() 
{
}

void SearchCacheTaskItem::Run()
{
    ReportMetrics();
}

void SearchCacheTaskItem::DeclareMetrics()
{
    IE_INIT_METRIC_GROUP(mMetricProvider, SearchCacheMemUse,
                         "global/SearchCacheMemUse", kmonitor::STATUS, "byte");
}

void SearchCacheTaskItem::ReportMetrics()
{
    IE_REPORT_METRIC(SearchCacheMemUse, mSearchCache->GetUsage());
    mSearchCache->ReportMetrics();
}

IE_NAMESPACE_END(util);

