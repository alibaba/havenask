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
#pragma once

#include <stdint.h>

#include "autil/Log.h"

namespace indexlib::util {
class BuildResourceMetricsNode;
}

namespace indexlibv2::index {

class BuildingIndexMemoryUseUpdater
{
public:
    BuildingIndexMemoryUseUpdater(indexlib::util::BuildResourceMetricsNode* buildResourceMetricsNode)
        : _buildResourceMetricsNode(buildResourceMetricsNode)
    {
    }
    ~BuildingIndexMemoryUseUpdater() = default;

    BuildingIndexMemoryUseUpdater(const BuildingIndexMemoryUseUpdater&) = delete;
    BuildingIndexMemoryUseUpdater& operator=(const BuildingIndexMemoryUseUpdater&) = delete;
    BuildingIndexMemoryUseUpdater(BuildingIndexMemoryUseUpdater&&) = delete;
    BuildingIndexMemoryUseUpdater& operator=(BuildingIndexMemoryUseUpdater&&) = delete;

    // update building index current memory use
    void UpdateCurrentMemUse(int64_t memUse);
    // estimate dump temp memory use, when dump finish, memory will released
    void EstimateDumpTmpMemUse(int64_t dumpTmpMemUse);
    // estaimate dump expand memory use, when dump finish, memory will not be released
    void EstimateDumpExpandMemUse(int64_t dumpTmpMemUse);
    // estimate dumped file size
    void EstimateDumpedFileSize(int64_t dumpFileSize);

    void GetMemInfo(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse, int64_t& dumpFileSize);

private:
    indexlib::util::BuildResourceMetricsNode* _buildResourceMetricsNode;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
