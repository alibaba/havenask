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
#include "indexlib/index/kkv/common/KKVSegmentStatistics.h"

#include "indexlib/framework/SegmentMetrics.h"

namespace indexlibv2::index {

void KKVSegmentStatistics::Store(indexlib::framework::SegmentMetrics* metrics, const std::string& groupName) const
{
    metrics->Set<size_t>(groupName, KKV_SEGMENT_MEM_USE, totalMemoryUse);

    // pkey related
    metrics->Set<size_t>(groupName, KKV_PKEY_COUNT, pkeyCount);
    metrics->Set<size_t>(groupName, KKV_PKEY_MEM_USE, pkeyMemoryUse);
    // TODO (qisa.cb) strange code...
    metrics->SetKeyCount(pkeyCount);
    metrics->Set<double>(groupName, KKV_PKEY_MEM_RATIO, pkeyMemoryRatio);

    // skey related
    metrics->Set<size_t>(groupName, KKV_SKEY_COUNT, skeyCount);
    metrics->Set<double>(groupName, KKV_MAX_SKEY_COUNT, maxSKeyCount);
    metrics->Set<size_t>(groupName, KKV_SKEY_MEM_USE, skeyMemoryUse);
    metrics->Set<double>(groupName, KKV_SKEY_VALUE_MEM_RATIO, skeyValueMemoryRatio);
    metrics->Set<double>(groupName, KKV_SKEY_COMPRESS_RATIO_NAME, skeyCompressRatio);

    // value related
    metrics->Set<size_t>(groupName, KKV_MAX_VALUE_LEN, maxValueLen);
    metrics->Set<size_t>(groupName, KKV_VALUE_MEM_USE, valueMemoryUse);
    metrics->Set<double>(groupName, KKV_VALUE_COMPRESS_RATIO_NAME, valueCompressRatio);
}

#define RETURN_IF_FALSE(ret)                                                                                           \
    if (!(ret)) {                                                                                                      \
        return false;                                                                                                  \
    }

bool KKVSegmentStatistics::Load(const indexlib::framework::SegmentMetrics* metrics, const std::string& groupName)
{
    std::string targetGroupName = GetTargetGroupNameLegacy(groupName, metrics);
    auto groupMetrics = metrics->GetSegmentGroupMetrics(targetGroupName);
    RETURN_IF_FALSE(groupMetrics);

    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_SEGMENT_MEM_USE, totalMemoryUse));

    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_PKEY_COUNT, pkeyCount));
    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_PKEY_MEM_USE, pkeyMemoryUse));
    if (!groupMetrics->Get<double>(KKV_PKEY_MEM_RATIO, pkeyMemoryRatio)) {
        pkeyMemoryRatio = (double)pkeyMemoryUse / (double)totalMemoryUse;
    }

    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_SKEY_COUNT, skeyCount));
    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_MAX_SKEY_COUNT, maxSKeyCount));
    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_SKEY_MEM_USE, skeyMemoryUse));
    RETURN_IF_FALSE(groupMetrics->Get<double>(KKV_SKEY_VALUE_MEM_RATIO, skeyValueMemoryRatio));
    RETURN_IF_FALSE(groupMetrics->Get<double>(KKV_SKEY_COMPRESS_RATIO_NAME, skeyCompressRatio));

    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_MAX_VALUE_LEN, maxValueLen));
    RETURN_IF_FALSE(groupMetrics->Get<size_t>(KKV_VALUE_MEM_USE, valueMemoryUse));
    RETURN_IF_FALSE(groupMetrics->Get<double>(KKV_VALUE_COMPRESS_RATIO_NAME, valueCompressRatio));

    return true;
}
#undef RETURN_IF_FALSE

std::string KKVSegmentStatistics::GetTargetGroupNameLegacy(const std::string& groupName,
                                                           const indexlib::framework::SegmentMetrics* metrics)
{
    size_t tmpTotalMemUse;
    // check target groupName exist
    if (metrics->Get<size_t>(groupName, KKV_SEGMENT_MEM_USE, tmpTotalMemUse)) {
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
} // namespace indexlibv2::index
