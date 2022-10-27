#include <autil/StringUtil.h>
#include "indexlib/util/cache/search_cache_creator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SearchCacheCreator);

SearchCacheCreator::SearchCacheCreator() 
{
}

SearchCacheCreator::~SearchCacheCreator() 
{
}

SearchCache* SearchCacheCreator::Create(const string& param,
                                        const MemoryQuotaControllerPtr& memoryQuotaController,
                                        const TaskSchedulerPtr& taskScheduler,
                                        misc::MetricProviderPtr metricProvider)
{
    if (param.empty())
    {
        IE_LOG(WARN, "can not create search cache with empty param, will disable search cache.");
        return NULL;
    }
    vector<vector<string>> paramVec;
    StringUtil::fromString(param, paramVec, "=", ";");
    int32_t cacheSize = -1; // MB
    int32_t numShardBits = 6; // 64 shards
    for (size_t i = 0; i < paramVec.size(); ++i)
    {
        if (paramVec[i].size() != 2)
        {
            break;
        }
        if (paramVec[i][0] == "cache_size"
            && StringUtil::fromString(paramVec[i][1], cacheSize))
        {
            continue;
        }
        else if (paramVec[i][0] == "num_shard_bits"
                 && StringUtil::fromString(paramVec[i][1], numShardBits))
        {
            continue;
        }
        else
        {
            break;
        }
    }
    if (numShardBits < 0 || numShardBits > 30)
    {
        IE_LOG(WARN, "parse search cache param[%s] failed, num_shard_bits[%d]",
               param.c_str(), numShardBits);
        return nullptr;
    }
    if (cacheSize == 0)
    {
        IE_LOG(WARN, "init search cache with param[%s], disable it", param.c_str());
        return nullptr;
    }
    else if (cacheSize < 0)
    {
        IE_LOG(WARN, "parse search cache param[%s] failed, cache_size[%dMB]",
               param.c_str(), cacheSize);
        return nullptr;
    }

    IE_LOG(INFO, "init search cache with param[%s], cacheSize[%dMB]",
           param.c_str(), cacheSize);
    return new SearchCache((uint64_t)cacheSize * 1024 * 1024,
                           memoryQuotaController, taskScheduler,
                           metricProvider, numShardBits);
}

IE_NAMESPACE_END(util);

