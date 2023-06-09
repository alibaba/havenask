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
#include "indexlib/index/kv/SegmentStatistics.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/KVCommonDefine.h"

namespace indexlibv2::index {

void SegmentStatistics::Store(indexlib::framework::SegmentMetrics* metrics, const std::string& groupName) const
{
    metrics->Set<size_t>(groupName, KV_SEGMENT_MEM_USE, totalMemoryUse);
    metrics->Set<float>(groupName, KV_KEY_VALUE_MEM_RATIO, keyValueSizeRatio);

    // key related
    metrics->Set<size_t>(groupName, KV_HASH_MEM_USE, keyMemoryUse);
    metrics->Set<int32_t>(groupName, KV_HASH_OCCUPANCY_PCT, occupancyPct);
    metrics->Set<size_t>(groupName, KV_KEY_COUNT, keyCount);
    metrics->SetKeyCount(keyCount);
    metrics->Set<size_t>(groupName, KV_KEY_DELETE_COUNT, deletedKeyCount);
    // value
    metrics->Set<size_t>(groupName, KV_VALUE_MEM_USE, valueMemoryUse);
    metrics->Set<float>(groupName, KV_VALUE_COMPRESSION_RATIO, valueCompressRatio);
}

#define RETURN_IF_FALSE(ret)                                                                                           \
    if (!(ret)) {                                                                                                      \
        return false;                                                                                                  \
    }

std::string SegmentStatistics::GetTargetGroupNameLegacy(const std::string& groupName,
                                                        const indexlib::framework::SegmentMetrics* metrics)
{
    size_t tmpTotalMemUse;
    // check target groupName exist
    if (metrics->Get<size_t>(groupName, KV_SEGMENT_MEM_USE, tmpTotalMemUse)) {
        return groupName;
    }
    std::vector<std::string> groupNames;
    metrics->ListAllGroupNames(groupNames);
    std::string legacyStatMetricsNamePrefix = "segment_stat_metrics_@_";
    for (size_t i = 0; i < groupNames.size(); i++) {
        auto found = groupNames[i].find(legacyStatMetricsNamePrefix);
        if (found != std::string::npos) {
            return groupNames[i];
        }
    }
    return groupName;
}

bool SegmentStatistics::Load(const indexlib::framework::SegmentMetrics* metrics, const std::string& groupName)
{
    std::string targetGroupName = GetTargetGroupNameLegacy(groupName, metrics);
    RETURN_IF_FALSE(metrics->Get<size_t>(targetGroupName, KV_SEGMENT_MEM_USE, totalMemoryUse));
    RETURN_IF_FALSE(metrics->Get<float>(targetGroupName, KV_KEY_VALUE_MEM_RATIO, keyValueSizeRatio));
    RETURN_IF_FALSE(metrics->Get<size_t>(targetGroupName, KV_HASH_MEM_USE, keyMemoryUse));
    RETURN_IF_FALSE(metrics->Get<int32_t>(targetGroupName, KV_HASH_OCCUPANCY_PCT, occupancyPct));
    RETURN_IF_FALSE(metrics->Get<size_t>(targetGroupName, KV_KEY_COUNT, keyCount));
    metrics->Get<size_t>(targetGroupName, KV_KEY_DELETE_COUNT, deletedKeyCount);
    RETURN_IF_FALSE(metrics->Get<size_t>(targetGroupName, KV_VALUE_MEM_USE, valueMemoryUse));
    metrics->Get<float>(targetGroupName, KV_VALUE_COMPRESSION_RATIO, valueCompressRatio);
    return true;
}

#undef RETURN_IF_FALSE

} // namespace indexlibv2::index
