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
#include "indexlib/merger/split_strategy/temperature_split_strategy.h"

#include "autil/StringUtil.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TemperatureSplitStrategy);

const std::string TemperatureSplitStrategy::STRATEGY_NAME = "temperature";

bool TemperatureSplitStrategy::Init(const util::KeyValueMap& parameters)
{
    if (parameters.size() == 0) {
        return true;
    }
    if (!CalculateDocSize(parameters)) {
        return false;
    }
    IE_LOG(INFO, "mMaxDocCount = [%lu, %lu, %lu]", mMaxDocCount[0], mMaxDocCount[1], mMaxDocCount[2]);
    return true;
}

bool TemperatureSplitStrategy::CalculateDocSize(const util::KeyValueMap& parameters)
{
    int64_t totalSize = 0;
    int64_t totalDocCount = 0;
    for (const auto& segMergeInfo : mSegMergeInfos) {
        if (segMergeInfo.segmentInfo.docCount == 0) {
            continue;
        }
        totalDocCount += segMergeInfo.segmentInfo.docCount;
        totalSize += segMergeInfo.segmentSize;
    }
    if (totalDocCount == 0 || totalSize == 0) {
        IE_LOG(WARN, "totalDocCount[%ld], totalSize[%ld], output segment doc count limit is max()", totalDocCount,
               totalSize);
        return true;
    }

    string maxHotSegmentSizeStr = GetValueFromKeyValueMap(parameters, "max_hot_segment_size");
    if (!maxHotSegmentSizeStr.empty()) {
        int64_t maxHotSegmentSize = 0;
        if (!StringUtil::fromString(maxHotSegmentSizeStr, maxHotSegmentSize)) {
            IE_LOG(ERROR, "max_hot_segment_size[%s] convert error!", maxHotSegmentSizeStr.c_str());
            return false;
        }
        mMaxDocCount[0] = ((double)maxHotSegmentSize * SIZE_MB / totalSize * totalDocCount);
        IE_LOG(INFO, "maxHotSegmentSize = [%ld], totalSize = [%ld], totalDocCount = [%ld]", maxHotSegmentSize,
               totalSize, totalDocCount);
    }

    string maxWarmSegmentSizeStr = GetValueFromKeyValueMap(parameters, "max_warm_segment_size");
    if (!maxWarmSegmentSizeStr.empty()) {
        int64_t maxWarmSegmentSize = 0;
        if (!StringUtil::fromString(maxWarmSegmentSizeStr, maxWarmSegmentSize)) {
            IE_LOG(ERROR, "max_warm_segment_size[%s] convert error!", maxWarmSegmentSizeStr.c_str());
            return false;
        }
        mMaxDocCount[1] = ((double)maxWarmSegmentSize * SIZE_MB / totalSize * totalDocCount);
        IE_LOG(INFO, "maxWarmSegmentSize = [%ld], totalSize = [%ld], totalDocCount = [%ld]", maxWarmSegmentSize,
               totalSize, totalDocCount);
    }

    string maxColdSegmentSizeStr = GetValueFromKeyValueMap(parameters, "max_cold_segment_size");
    if (!maxColdSegmentSizeStr.empty()) {
        int64_t maxColdSegmentSize = 0;
        if (!StringUtil::fromString(maxColdSegmentSizeStr, maxColdSegmentSize)) {
            IE_LOG(ERROR, "max_cold_segment_size[%s] convert error!", maxColdSegmentSizeStr.c_str());
            return false;
        }
        mMaxDocCount[2] = ((double)maxColdSegmentSize * SIZE_MB / totalSize * totalDocCount);
        IE_LOG(INFO, "maxColdSegmentSize = [%ld], totalSize = [%ld], totalDocCount = [%ld]", maxColdSegmentSize,
               totalSize, totalDocCount);
    }
    return true;
}

index_base::SegmentMergeInfo TemperatureSplitStrategy::GetSegmentMergeInfo(segmentid_t segmentId)
{
    for (auto segmentMergeInfo : mSegMergeInfos) {
        if (segmentMergeInfo.segmentId == segmentId) {
            return segmentMergeInfo;
        }
    }
    assert(false);
    index_base::SegmentMergeInfo info;
    return info;
}

segmentindex_t TemperatureSplitStrategy::DoProcess(segmentid_t oldSegId, docid_t oldLocalId, int64_t& hintValue)
{
    auto iter = mHintDocInfos.find(oldSegId);
    KeyValueMap params;
    TemperatureProperty property;
    if (iter != mHintDocInfos.end()) {
        MultiSegmentMetricsUpdaterPtr multiUpdater = static_pointer_cast<MultiSegmentMetricsUpdater>(iter->second);
        SegmentMetricsUpdaterPtr baseUpdater =
            multiUpdater->GetMetricUpdate(TemperatureSegmentMetricsUpdater::UPDATER_NAME);
        assert(baseUpdater);
        TemperatureSegmentMetricsUpdaterPtr temperatureUpdater =
            static_pointer_cast<TemperatureSegmentMetricsUpdater>(baseUpdater);
        if (!temperatureUpdater) {
            INDEXLIB_FATAL_ERROR(Runtime, "cannot get temperatureUpdater in hintUpdates");
        }
        property = temperatureUpdater->GetDocTemperatureProperty(oldSegId, oldLocalId);
    } else if (mUpdaters.find(oldSegId) == mUpdaters.end()) {
        TemperatureSegmentMetricsUpdaterPtr temperatureUpdater(new TemperatureSegmentMetricsUpdater(nullptr));
        if (!temperatureUpdater->InitForReCaculator(mSchema, GetSegmentMergeInfo(oldSegId), mReaderContainer,
                                                    DeletionMapReaderPtr(), oldSegId, params)) {
            INDEXLIB_FATAL_ERROR(Runtime, "temperature update init failed for segment [%d]", oldSegId);
        }
        property = temperatureUpdater->GetDocTemperatureProperty(oldSegId, oldLocalId);
        mUpdaters.insert(make_pair(oldSegId, temperatureUpdater));
    } else {
        property = mUpdaters.find(oldSegId)->second->GetDocTemperatureProperty(oldSegId, oldLocalId);
    }
    hintValue = property;
    int32_t propertyIdx = property;
    if (mCurrentDocCount[propertyIdx] >= mMaxDocCount[propertyIdx]) {
        mMaxSegmentId++;
        IE_LOG(INFO, "maxSegmentId increase, now maxSegmentId = [%d]", mMaxSegmentId);
        mOutputSegments[propertyIdx] = mMaxSegmentId;
        mCurrentDocCount[propertyIdx] = 0;
    }
    mCurrentDocCount[propertyIdx]++;
    return mOutputSegments[propertyIdx];
}
}} // namespace indexlib::merger
