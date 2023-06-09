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
#ifndef __INDEXLIB_TEMPERATURE_SPLIT_STRATEGY_H
#define __INDEXLIB_TEMPERATURE_SPLIT_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

namespace indexlib { namespace merger {

/*
  "merged_segment_split_strategy": {
      "class_name": "temperature",
       "parameters" : {
          "max_hot_segment_size" : "10000",
          "max_warm_segment_size" : "100000",
          "max_cold_segment_size" : "1000000"
        }
  }
 */

class TemperatureSplitStrategy : public SplitSegmentStrategy
{
public:
    TemperatureSplitStrategy(index::SegmentDirectoryBasePtr segDir, config::IndexPartitionSchemaPtr schema,
                             index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                             const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
                             std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                             const util::MetricProviderPtr& metrics)
        : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan, hintDocInfos, metrics)
        , mMaxSegmentId(-1)
    {
        for (int i = 0; i < 3; i++) {
            mMaxDocCount.push_back(std::numeric_limits<uint64_t>::max());
            mCurrentDocCount.push_back(std::numeric_limits<uint64_t>::max());
        }
    }

    ~TemperatureSplitStrategy() {}

public:
    bool Init(const util::KeyValueMap& parameters) override;
    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId, int64_t& hintValue) override;

private:
    bool CalculateDocSize(const util::KeyValueMap& parameters);
    index_base::SegmentMergeInfo GetSegmentMergeInfo(segmentid_t segmentId);

public:
    static const std::string STRATEGY_NAME;

private:
    std::vector<uint64_t> mMaxDocCount;
    std::map<int32_t, segmentindex_t> mOutputSegments;
    std::vector<uint64_t> mCurrentDocCount;
    std::map<segmentid_t, index::TemperatureSegmentMetricsUpdaterPtr> mUpdaters;
    int32_t mMaxSegmentId;
    static const int64_t SIZE_MB = 1024 * 1024;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureSplitStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_TEMPERATURE_SPLIT_STRATEGY_H
