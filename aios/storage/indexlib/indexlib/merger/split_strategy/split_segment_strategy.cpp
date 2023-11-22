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
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricType.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SplitSegmentStrategy);

SplitSegmentStrategy::SplitSegmentStrategy(index::SegmentDirectoryBasePtr segDir,
                                           config::IndexPartitionSchemaPtr schema,
                                           index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                                           const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
                                           std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                                           const util::MetricProviderPtr& metrics)
    : mSegDir(segDir)
    , mSchema(schema)
    , mReaderContainer(attrReaders)
    , mSegMergeInfos(segMergeInfos)
    , mPlan(plan)
    , mHintDocInfos(hintDocInfos)
    , mProcessLatencyInMs(0)
    , mMetricsProvider(metrics)
{
    IE_INIT_METRIC_GROUP(mMetricsProvider, splitLatency, "merge/splitLatency", kmonitor::GAUGE, "count");
}

SplitSegmentStrategy::~SplitSegmentStrategy() { IE_REPORT_METRIC(splitLatency, mProcessLatencyInMs * 1.0 / 1000000); }

segmentindex_t SplitSegmentStrategy::Process(segmentid_t oldSegId, docid_t oldLocalId)
{
    int64_t startTime = TimeUtility::currentTimeInMicroSeconds();
    int64_t hintValue = -1;
    segmentindex_t segIdx = DoProcess(oldSegId, oldLocalId, hintValue);
    auto currentSize = mMetricsUpdaters.size();
    if (segIdx >= currentSize) {
        IE_LOG(INFO, "add new MetricsUpdater, segIdx = [%d], currentSize = [%d]", (int)segIdx, (int)currentSize);
        for (size_t i = currentSize; i <= segIdx; ++i) {
            mMetricsUpdaters.push_back(mUpdaterGenerator());
        }
    }
    const auto& updater = mMetricsUpdaters[segIdx];
    if (updater) {
        updater->UpdateForMerge(oldSegId, oldLocalId, hintValue);
    }
    mProcessLatencyInMs += TimeUtility::currentTimeInMicroSeconds() - startTime;
    return segIdx;
}

std::vector<autil::legacy::json::JsonMap> SplitSegmentStrategy::GetSegmentCustomizeMetrics()
{
    std::vector<autil::legacy::json::JsonMap> jsonMaps;
    if (mMetricsUpdaters.size() == 0) {
        const auto& updater = mUpdaterGenerator();
        if (updater) {
            return {updater->Dump()};
        } else {
            return {autil::legacy::json::JsonMap()};
        }
    }
    jsonMaps.reserve(mMetricsUpdaters.size());
    for (const auto& updater : mMetricsUpdaters) {
        if (updater) {
            jsonMaps.push_back(updater->Dump());
        } else {
            jsonMaps.push_back(autil::legacy::json::JsonMap());
        }
    }
    return jsonMaps;
}

bool SplitSegmentStrategy::GetTargetSegmentTemperatureMeta(size_t idx, index_base::SegmentTemperatureMeta& meta)
{
    if (idx >= mMetricsUpdaters.size()) {
        // empty segment push defaultMeta
        meta = index_base::SegmentTemperatureMeta::GenerateDefaultSegmentMeta();
        return true;
    }
    MultiSegmentMetricsUpdaterPtr multiUpdater = static_pointer_cast<MultiSegmentMetricsUpdater>(mMetricsUpdaters[idx]);
    SegmentMetricsUpdaterPtr baseUpdater =
        multiUpdater->GetMetricUpdate(TemperatureSegmentMetricsUpdater::UPDATER_NAME);
    if (!baseUpdater) {
        IE_LOG(ERROR, "cannot get temperature mertric updater");
        return false;
    }
    TemperatureSegmentMetricsUpdaterPtr temperatureUpdater =
        static_pointer_cast<TemperatureSegmentMetricsUpdater>(baseUpdater);
    assert(temperatureUpdater);
    temperatureUpdater->GetSegmentTemperatureMeta(meta);
    return true;
}
}} // namespace indexlib::merger
