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

#include "indexlib/base/Constant.h"

namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
class MetricsManager;
class SegmentInfo;
} // namespace indexlibv2::framework

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::index {

struct DiskIndexerParameter {
    segmentid_t segmentId = INVALID_SEGMENTID;
    uint64_t docCount = 0;
    framework::MetricsManager* metricsManager = nullptr;
    framework::IIndexMemoryReclaimer* indexMemoryReclaimer = nullptr;
    std::shared_ptr<framework::SegmentInfo> segmentInfo;
    std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics;
    enum {
        READER_NORMAL,        // normal mode
        READER_DEFAULT_VALUE, // not physical index, for default value
    } readerOpenType = READER_NORMAL;
};

} // namespace indexlibv2::index
