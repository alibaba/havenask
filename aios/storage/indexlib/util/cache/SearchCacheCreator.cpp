/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/util/cache/SearchCacheCreator.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, SearchCacheCreator);

SearchCacheCreator::SearchCacheCreator() {}

SearchCacheCreator::~SearchCacheCreator() {}

SearchCache* SearchCacheCreator::Create(const string& param, const MemoryQuotaControllerPtr& memoryQuotaController,
                                        const std::shared_ptr<TaskScheduler>& taskScheduler,
                                        util::MetricProviderPtr metricProvider)
{
    if (param.empty()) {
        AUTIL_LOG(WARN, "can not create search cache with empty param, will disable search cache.");
        return NULL;
    }
    vector<vector<string>> paramVec;
    StringUtil::fromString(param, paramVec, "=", ";");
    int32_t cacheSize = -1;   // MB
    int32_t numShardBits = 6; // 64 shards
    float highPriorityRatio = 0.0f;
    for (size_t i = 0; i < paramVec.size(); ++i) {
        if (paramVec[i].size() != 2) {
            break;
        }
        if (paramVec[i][0] == "cache_size" && StringUtil::fromString(paramVec[i][1], cacheSize)) {
            continue;
        } else if (paramVec[i][0] == "num_shard_bits" && StringUtil::fromString(paramVec[i][1], numShardBits)) {
            continue;
        } else if (paramVec[i][0] == "lru_high_priority_ratio" &&
                   StringUtil::fromString(paramVec[i][1], highPriorityRatio)) {
            continue;
        } else {
            break;
        }
    }

    if (highPriorityRatio < 0 || highPriorityRatio > 1.0) {
        AUTIL_LOG(WARN, "parse search cache param [%s] failed, high_priority_ratio[%f]", param.c_str(),
                  highPriorityRatio);
        return nullptr;
    }
    if (numShardBits < 0 || numShardBits >= 20) {
        AUTIL_LOG(WARN, "parse search cache param[%s] failed, num_shard_bits[%d]", param.c_str(), numShardBits);
        return nullptr;
    }
    if (cacheSize == 0) {
        AUTIL_LOG(WARN, "init search cache with param[%s], disable it", param.c_str());
        return nullptr;
    } else if (cacheSize < 0) {
        AUTIL_LOG(WARN, "parse search cache param[%s] failed, cache_size[%dMB]", param.c_str(), cacheSize);
        return nullptr;
    }

    AUTIL_LOG(INFO, "init search cache with param[%s], cacheSize[%dMB]", param.c_str(), cacheSize);
    return new SearchCache((uint64_t)cacheSize * 1024 * 1024, memoryQuotaController, taskScheduler, metricProvider,
                           numShardBits, highPriorityRatio);
}
}} // namespace indexlib::util
