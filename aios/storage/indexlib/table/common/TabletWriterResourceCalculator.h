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
#include <memory>

#include "autil/Log.h"

namespace indexlib::util {
class BuildResourceMetrics;
}

namespace indexlib::table {

class TabletWriterResourceCalculator
{
public:
    TabletWriterResourceCalculator(const std::shared_ptr<util::BuildResourceMetrics>& buildingSegmentMetrics,
                                   bool isOnline, int64_t dumpThreadCount);
    ~TabletWriterResourceCalculator() {}

public:
    int64_t GetCurrentMemoryUse() const;
    int64_t EstimateMaxMemoryUseOfCurrentSegment() const;
    int64_t EstimateDumpMemoryUse() const;
    int64_t EstimateDumpFileSize() const;

private:
    bool _isOnline = true;
    int64_t _dumpThreadCount = 0;
    std::shared_ptr<util::BuildResourceMetrics> _buildingSegmentMetrics;

private:
    friend class TabletWriterResourceCalculatorTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
