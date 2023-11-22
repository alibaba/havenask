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
#include "indexlib/framework/LSMMemTableControlStrategy.h"

#include <string>

#include "autil/UnitUtil.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, LSMMemTableControlStrategy);

LSMMemTableControlStrategy::LSMMemTableControlStrategy(
    const std::shared_ptr<indexlibv2::config::TabletOptions>& options,
    const std::shared_ptr<indexlibv2::MemoryQuotaSynchronizer>& buildMemoryQuotaSynchronizer)
    : indexlibv2::framework::DefaultMemoryControlStrategy(options, buildMemoryQuotaSynchronizer)
{
}

LSMMemTableControlStrategy::~LSMMemTableControlStrategy() {}

indexlibv2::framework::MemoryStatus LSMMemTableControlStrategy::CheckRealtimeIndexMemoryQuota(
    const std::shared_ptr<indexlibv2::framework::TabletMetrics>& tabletMetrics) const
{
    auto rtIndexMemsizeBytes =
        tabletMetrics->GetRtIndexMemSize(indexlibv2::framework::RealtimeIndexMemoryType::DUMPING) +
        tabletMetrics->GetRtIndexMemSize(indexlibv2::framework::RealtimeIndexMemoryType::BUILDING);
    if (rtIndexMemsizeBytes >= _tabletOptions->GetBuildMemoryQuota()) {
        // current tablet
        AUTIL_LOG(WARN, "table[%s] reach build memory quota, current rt-index memsize[%s] build memory quota[%s]",
                  _tabletOptions->GetTabletName().c_str(), autil::UnitUtil::GiBDebugString(rtIndexMemsizeBytes).c_str(),
                  autil::UnitUtil::GiBDebugString(_tabletOptions->GetBuildMemoryQuota()).c_str());
        return indexlibv2::framework::MemoryStatus::REACH_MAX_RT_INDEX_SIZE;
    }
    if (_buildMemoryQuotaSynchronizer->GetFreeQuota() <= 0) {
        // maybe multi-tablet shared the buildMemoryQuotaController
        AUTIL_LOG(WARN, "table[%s] reach total build memory quota, current free quota[%s]",
                  _tabletOptions->GetTabletName().c_str(),
                  autil::UnitUtil::GiBDebugString(_buildMemoryQuotaSynchronizer->GetFreeQuota()).c_str());
        return indexlibv2::framework::MemoryStatus::REACH_MAX_RT_INDEX_SIZE;
    }
    AUTIL_LOG(DEBUG, "table[%s] rtIndex memory size [%s]", _tabletOptions->GetTabletName().c_str(),
              autil::UnitUtil::GiBDebugString(rtIndexMemsizeBytes).c_str());
    return indexlibv2::framework::MemoryStatus::OK;
}

void LSMMemTableControlStrategy::SyncMemoryQuota(
    const std::shared_ptr<indexlibv2::framework::TabletMetrics>& tabletMetrics)
{
    auto rtIndexMemsizeBytes =
        tabletMetrics->GetRtIndexMemSize(indexlibv2::framework::RealtimeIndexMemoryType::DUMPING) +
        tabletMetrics->GetRtIndexMemSize(indexlibv2::framework::RealtimeIndexMemoryType::BUILDING);
    _buildMemoryQuotaSynchronizer->SyncMemoryQuota(rtIndexMemsizeBytes);
}

} // namespace indexlibv2::framework
