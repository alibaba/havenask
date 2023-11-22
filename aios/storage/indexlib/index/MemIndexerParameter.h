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
#include "indexlib/base/Types.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/index/IndexerDirectories.h"

namespace indexlib::util {
class BuildResourceMetrics;
}

namespace indexlibv2::config {
class TabletOptions;
}

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
class MetricsManager;
class SegmentInfo;
} // namespace indexlibv2::framework

namespace indexlibv2::index {

struct MemIndexerParameter {
    segmentid_t segmentId = INVALID_SEGMENTID; // for log/metrics
    int64_t maxMemoryUseInBytes = 0;
    framework::MetricsManager* metricsManager = nullptr;
    indexlib::util::BuildResourceMetrics* buildResourceMetrics = nullptr;
    indexlib::framework::SegmentMetrics* lastSegmentMetrics = nullptr;
    framework::IIndexMemoryReclaimer* indexMemoryReclaimer = nullptr; // only for kv/kkv
    bool isOnline = true;                                             // only for ann
    std::shared_ptr<framework::SegmentInfo> segmentInfo;
    std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics; // only for rocks
    config::SortDescriptions sortDescriptions;
    bool isTolerateDocError = true;
    std::string tabletName;                                 // only for orc
    std::shared_ptr<IndexerDirectories> indexerDirectories; // only for ann
    std::shared_ptr<const docid64_t> currentBuildDocId;
};

} // namespace indexlibv2::index
