#ifndef __INDEXLIB_SEARCH_CACHE_CREATOR_H
#define __INDEXLIB_SEARCH_CACHE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/cache/search_cache.h"

IE_NAMESPACE_BEGIN(util);

class SearchCacheCreator
{
public:
    SearchCacheCreator();
    ~SearchCacheCreator();
public:
    static SearchCache* Create(const std::string& param,
                               const MemoryQuotaControllerPtr& memoryQuotaController,
                               const TaskSchedulerPtr& taskScheduler,
                               misc::MetricProviderPtr metricProvider);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchCacheCreator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SEARCH_CACHE_CREATOR_H
