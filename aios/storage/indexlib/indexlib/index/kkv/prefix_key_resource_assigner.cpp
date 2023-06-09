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
#include "indexlib/index/kkv/prefix_key_resource_assigner.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PrefixKeyResourceAssigner);

PrefixKeyResourceAssigner::PrefixKeyResourceAssigner(const KKVIndexConfigPtr& kkvIndexConfig, uint32_t columnIdx,
                                                     uint32_t totalColumnCount)
    : mTotalMemQuota(0)
    , mKkvIndexConfig(kkvIndexConfig)
    , mColumnIdx(columnIdx)
    , mColumnCount(totalColumnCount)
{
    assert(columnIdx < totalColumnCount);
}

PrefixKeyResourceAssigner::~PrefixKeyResourceAssigner() {}

void PrefixKeyResourceAssigner::Init(const QuotaControlPtr& buildMemoryQuotaControler,
                                     const IndexPartitionOptions& options)
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

size_t PrefixKeyResourceAssigner::Assign(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics)
{
    IE_LOG(INFO, "Total memory quota is %lu", mTotalMemQuota);
    double totalPkeyRatio = 0; // memory ratio for all column pkey in total memory quota
    double curPkeyRatio = 0;   // memory ratio for pkey in current sharding column
    double curMemoryRatio = 0; // memory ratio for current sharding column in total memory quota
    GetDataRatio(metrics, totalPkeyRatio, curPkeyRatio, curMemoryRatio);
    double totalBalanceMemory = mTotalMemQuota * DEFAULT_BALANCE_STRATEGY_RATIO;
    double totalDynamicMemory = mTotalMemQuota * (1 - DEFAULT_BALANCE_STRATEGY_RATIO);
    double assignMem = ((double)totalBalanceMemory * totalPkeyRatio / mColumnCount) +
                       ((double)totalDynamicMemory * curMemoryRatio * curPkeyRatio);
    IE_LOG(INFO, "Assign [%lu] memory resource for prefix key [%u/%u]", (size_t)assignMem, mColumnIdx, mColumnCount);
    return (size_t)(assignMem);
}

void PrefixKeyResourceAssigner::GetDataRatio(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                                             double& totalPkeyRatio, double& curPkeyRatio, double& curMemoryRatio)
{
    // set default ratio
    totalPkeyRatio = DEFAULT_PKEY_MEMORY_USE_RATIO;
    curPkeyRatio = DEFAULT_PKEY_MEMORY_USE_RATIO;
    curMemoryRatio = (double)1 / mColumnCount;
    if (!metrics) {
        return;
    }

    size_t totalMemory = 0;
    size_t totalPkeyMemory = 0;
    size_t curMemory = 0;
    size_t curPkeyMemory = 0;
    for (uint32_t i = 0; i < mColumnCount; i++) {
        string groupName = SegmentWriter::GetMetricsGroupName(i);
        size_t pkeySize = 0;
        size_t memSize = 0;
        metrics->Get<size_t>(groupName, KKV_PKEY_MEM_USE, pkeySize);
        metrics->Get<size_t>(groupName, KKV_SEGMENT_MEM_USE, memSize);

        totalMemory += memSize;
        totalPkeyMemory += pkeySize;
        if (i == mColumnIdx) {
            curMemory = memSize;
            curPkeyMemory = pkeySize;
        }
    }

    if (totalPkeyMemory == 0 || totalMemory == 0 || totalPkeyMemory > totalMemory) {
        return;
    }
    totalPkeyRatio = (double)totalPkeyMemory / totalMemory;
    curMemoryRatio = (double)curMemory / totalMemory;
    curPkeyRatio = (curPkeyMemory <= curMemory && curMemory > 0) ? ((double)curPkeyMemory / curMemory)
                                                                 : DEFAULT_PKEY_MEMORY_USE_RATIO;
}

size_t PrefixKeyResourceAssigner::EstimatePkeyCount(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics)
{
    size_t assignMemorySize = Assign(metrics);
    string hashTypeStr = mKkvIndexConfig->GetIndexPreference().GetHashDictParam().GetHashType();
    int32_t occupancyPct = mKkvIndexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    PKeyTableType type = GetPrefixKeyTableType(hashTypeStr);

    switch (type) {
    case PKT_SEPARATE_CHAIN:
        return SeparateChainPrefixKeyTable<SKeyListInfo>::EstimateCapacity(PKOT_RW, assignMemorySize);
    case PKT_DENSE:
        return ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE>::EstimateCapacity(PKOT_RW, assignMemorySize,
                                                                                   occupancyPct);
    case PKT_CUCKOO:
        return ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE>::EstimateCapacity(PKOT_RW, assignMemorySize,
                                                                                   std::min(80, occupancyPct));
    default:
        assert(false);
    }
    return 0;
}
}} // namespace indexlib::index
