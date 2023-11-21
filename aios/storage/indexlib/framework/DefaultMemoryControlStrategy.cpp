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
#include "indexlib/framework/DefaultMemoryControlStrategy.h"

#include <algorithm>
#include <stddef.h>
#include <string>

#include "autil/UnitUtil.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, DefaultMemoryControlStrategy);

DefaultMemoryControlStrategy::DefaultMemoryControlStrategy(
    const std::shared_ptr<config::TabletOptions>& options,
    const std::shared_ptr<MemoryQuotaSynchronizer>& buildMemoryQuotaSynchronizer)
    : _tabletOptions(options)
    , _buildMemoryQuotaSynchronizer(buildMemoryQuotaSynchronizer)
{
}

DefaultMemoryControlStrategy::~DefaultMemoryControlStrategy() {}

MemoryStatus
DefaultMemoryControlStrategy::CheckRealtimeIndexMemoryQuota(const std::shared_ptr<TabletMetrics>& tabletMetrics) const
{
    auto rtIndexMemsizeBytes = tabletMetrics->GetRtIndexMemSize();
    if (rtIndexMemsizeBytes >= _tabletOptions->GetBuildMemoryQuota()) {
        // current tablet
        AUTIL_LOG(WARN, "table[%s] reach build memory quota, current rt-index memsize[%s] build memory quota[%s]",
                  _tabletOptions->GetTabletName().c_str(), autil::UnitUtil::GiBDebugString(rtIndexMemsizeBytes).c_str(),
                  autil::UnitUtil::GiBDebugString(_tabletOptions->GetBuildMemoryQuota()).c_str());
        return MemoryStatus::REACH_MAX_RT_INDEX_SIZE;
    }
    if (_buildMemoryQuotaSynchronizer->GetFreeQuota() <= 0) {
        // maybe multi-tablet shared the buildMemoryQuotaController
        AUTIL_LOG(WARN, "table[%s] reach total build memory quota, current free quota[%s]",
                  _tabletOptions->GetTabletName().c_str(),
                  autil::UnitUtil::GiBDebugString(_buildMemoryQuotaSynchronizer->GetFreeQuota()).c_str());
        return MemoryStatus::REACH_MAX_RT_INDEX_SIZE;
    }
    AUTIL_LOG(DEBUG, "table[%s] rtIndex memory size [%s]", _tabletOptions->GetTabletName().c_str(),
              autil::UnitUtil::GiBDebugString(rtIndexMemsizeBytes).c_str());
    return MemoryStatus::OK;
}

MemoryStatus
DefaultMemoryControlStrategy::CheckTotalMemoryQuota(const std::shared_ptr<TabletMetrics>& tabletMetrics) const
{
    size_t availableMemoryUse = tabletMetrics->GetFreeQuota();
    size_t buildingSegmentDumpExpandMemsize = tabletMetrics->GetBuildingSegmentDumpExpandMemsize();
    size_t dumpingSegmentDumpExpandMemsize = tabletMetrics->GetMaxDumpingSegmentExpandMemsize();
    size_t dumpExpandMemUse = std::max(buildingSegmentDumpExpandMemsize, dumpingSegmentDumpExpandMemsize);
    if (availableMemoryUse <= dumpExpandMemUse) {
        size_t currentMemUse = tabletMetrics->GetTabletMemoryUse();
        AUTIL_LOG(WARN,
                  "table[%s] reach memory use limit free mem [%s], current total memory is [%s], dumpExpandMemUse [%s]",
                  _tabletOptions->GetTabletName().c_str(), autil::UnitUtil::GiBDebugString(availableMemoryUse).c_str(),
                  autil::UnitUtil::GiBDebugString(currentMemUse).c_str(),
                  autil::UnitUtil::GiBDebugString(dumpExpandMemUse).c_str());
        return MemoryStatus::REACH_TOTAL_MEM_LIMIT;
    }
    AUTIL_LOG(DEBUG,
              "table[%s] available memory size [%s], "
              "building segment dump expand memory size [%s], "
              "dumping segment dump expand memory size [%s], "
              "dump expand memory size [%s]",
              _tabletOptions->GetTabletName().c_str(), autil::UnitUtil::GiBDebugString(availableMemoryUse).c_str(),
              autil::UnitUtil::GiBDebugString(buildingSegmentDumpExpandMemsize).c_str(),
              autil::UnitUtil::GiBDebugString(dumpingSegmentDumpExpandMemsize).c_str(),
              autil::UnitUtil::GiBDebugString(dumpExpandMemUse).c_str());
    return MemoryStatus::OK;
}

void DefaultMemoryControlStrategy::SyncMemoryQuota(const std::shared_ptr<TabletMetrics>& tabletMetrics)
{
    _buildMemoryQuotaSynchronizer->SyncMemoryQuota(tabletMetrics->GetRtIndexMemSize());
}

} // namespace indexlibv2::framework
