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

namespace indexlib::framework {
class SegmentMetrics;
} // namespace indexlib::framework

namespace indexlib::util {
class CounterMap;
class BuildResourceMetrics;
} // namespace indexlib::util

namespace indexlibv2 { namespace framework {
class IIndexMemoryReclaimer;
class MetricsManager;
class SegmentInfo;
}} // namespace indexlibv2::framework

namespace indexlibv2::config {
class TabletOptions;
}

namespace indexlibv2::index {

struct IndexerParameter {
    using SortPatternFunc = std::function<config::SortPattern(const std::string&)>;

    schemaid_t readerSchemaId = DEFAULT_SCHEMAID;
    segmentid_t segmentId = INVALID_SEGMENTID;
    uint64_t docCount = 0;
    int64_t maxMemoryUseInBytes = 0;
    std::shared_ptr<indexlib::util::CounterMap> counterMap;
    std::string counterPrefix;
    framework::MetricsManager* metricsManager = nullptr;
    indexlib::util::BuildResourceMetrics* buildResourceMetrics = nullptr;
    indexlib::framework::SegmentMetrics* lastSegmentMetrics = nullptr;
    framework::IIndexMemoryReclaimer* indexMemoryReclaimer = nullptr;
    // unbind between normal table and attribute indexer
    SortPatternFunc sortPatternFunc;
    bool disableUpdate = false;

    bool isOnline = true;
    std::shared_ptr<framework::SegmentInfo> segmentInfo;
    std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics;

    config::SortDescriptions sortDescriptions;
    enum {
        READER_NORMAL,        // normal mode
        READER_DEFAULT_VALUE, // not physical index, for default value
    } readerOpenType = READER_NORMAL;

    const config::TabletOptions* tabletOptions = nullptr;
    std::shared_ptr<IndexerDirectories> indexerDirectories;
};

} // namespace indexlibv2::index
