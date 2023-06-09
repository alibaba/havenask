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
#include "indexlib/index/kv/kv_resource_assigner.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvResourceAssigner);

KvResourceAssigner::KvResourceAssigner(const KVIndexConfigPtr& kvIndexConfig, uint32_t columnIdx,
                                       uint32_t totalColumnCount)
    : mTotalMemQuota(0)
    , mKvIndexConfig(kvIndexConfig)
    , mColumnIdx(columnIdx)
    , mColumnCount(totalColumnCount)
{
    assert(columnIdx < totalColumnCount);
}

KvResourceAssigner::~KvResourceAssigner() {}

void KvResourceAssigner::Init(const QuotaControlPtr& buildMemoryQuotaControler, const IndexPartitionOptions& options)
{
    mTotalMemQuota = options.GetBuildConfig().buildTotalMemory * 1024 * 1024;
    if (buildMemoryQuotaControler) {
        mTotalMemQuota = buildMemoryQuotaControler->GetLeftQuota();
    }

    if (options.IsOffline()) {
        mTotalMemQuota >>= 1; // for async dump, use half only
    }

    uint64_t reserve = (uint64_t)(mTotalMemQuota * MAX_MEMORY_USE_RESERVE_RATIO);
    if (reserve < MAX_MEMORY_USE_RESERVE_MIN) {
        reserve = MAX_MEMORY_USE_RESERVE_MIN;
    }

    if (mTotalMemQuota < reserve) {
        IE_LOG(ERROR, "leftQuota[%lu] small than reserve[%lu]", mTotalMemQuota, reserve);
        mTotalMemQuota = 0;
    } else {
        mTotalMemQuota -= reserve;
    }
}

size_t KvResourceAssigner::Assign(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics)
{
    IE_LOG(INFO, "Total memory quota is %lu", mTotalMemQuota);
    double curMemRatio = 0;
    GetDataRatio(metrics, curMemRatio);
    double totalBalanceMemory = mTotalMemQuota * DEFAULT_BALANCE_STRATEGY_RATIO;
    double totalDynamicMemory = mTotalMemQuota * (1 - DEFAULT_BALANCE_STRATEGY_RATIO);
    double memQuota = ((double)totalBalanceMemory / mColumnCount) + ((double)totalDynamicMemory * curMemRatio);
    IE_LOG(INFO, "Assign [%lu] memory resource for kv writer [%u/%u]", (size_t)memQuota, mColumnIdx, mColumnCount);
    return (size_t)memQuota;
}

void KvResourceAssigner::GetDataRatio(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                                      double& curMemoryRatio)
{
    // set default ratio
    curMemoryRatio = (double)1 / mColumnCount;
    if (!metrics) {
        return;
    }

    size_t totalMemory = 0;
    size_t curMemory = 0;
    for (uint32_t i = 0; i < mColumnCount; i++) {
        string groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(i);
        size_t memSize = 0;
        metrics->Get<size_t>(groupName, KV_SEGMENT_MEM_USE, memSize);
        totalMemory += memSize;
        if (i == mColumnIdx) {
            curMemory = memSize;
        }
    }
    if (curMemory > 0 && curMemory <= totalMemory) {
        curMemoryRatio = (double)curMemory / totalMemory;
    }
}
}} // namespace indexlib::index
