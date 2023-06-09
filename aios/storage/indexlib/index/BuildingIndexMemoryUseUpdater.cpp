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
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"

#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, BuildingIndexMemoryUseUpdater);

void BuildingIndexMemoryUseUpdater::UpdateCurrentMemUse(int64_t memUse)
{
    _buildResourceMetricsNode->Update(indexlib::util::BMT_CURRENT_MEMORY_USE, memUse);
}
void BuildingIndexMemoryUseUpdater::EstimateDumpTmpMemUse(int64_t dumpTmpMemUse)
{
    _buildResourceMetricsNode->Update(indexlib::util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTmpMemUse);
}
void BuildingIndexMemoryUseUpdater::EstimateDumpExpandMemUse(int64_t dumpExpandMemUse)
{
    _buildResourceMetricsNode->Update(indexlib::util::BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandMemUse);
}
void BuildingIndexMemoryUseUpdater::EstimateDumpedFileSize(int64_t dumpFileSize)
{
    _buildResourceMetricsNode->Update(indexlib::util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

void BuildingIndexMemoryUseUpdater::GetMemInfo(int64_t& currentMemUse, int64_t& dumpTmpMemUse,
                                               int64_t& dumpExpandMemUse, int64_t& dumpFileSize)
{
    currentMemUse = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_CURRENT_MEMORY_USE);
    dumpTmpMemUse = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_DUMP_TEMP_MEMORY_SIZE);
    dumpExpandMemUse = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_DUMP_EXPAND_MEMORY_SIZE);
    dumpFileSize = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_DUMP_FILE_SIZE);
}

} // namespace indexlibv2::index
