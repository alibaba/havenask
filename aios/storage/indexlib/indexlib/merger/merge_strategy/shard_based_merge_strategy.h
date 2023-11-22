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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class ShardBasedMergeStrategy : public MergeStrategy
{
public:
    ShardBasedMergeStrategy(const merger::SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema);
    ~ShardBasedMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(ShardBasedMergeStrategy, SHARD_BASED_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;
    std::string GetParameter() const override;
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;
    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

private:
    std::vector<std::vector<bool>>
    GenerateMergeTag(const indexlibv2::framework::LevelInfo& levelInfo,
                     const std::map<segmentid_t, index_base::SegmentMergeInfo>& segMergeInfoMap);

    MergeTask GenerateMergeTask(const indexlibv2::framework::LevelInfo& levelInfo,
                                const std::map<segmentid_t, index_base::SegmentMergeInfo>& segMergeInfoMap,
                                const std::vector<std::vector<bool>>& mergeTag);
    uint32_t CalculateLevelPercent(double spaceAmplification, uint32_t levelNum) const;
    double CalculateSpaceAmplification(uint32_t percent, uint32_t levelNum) const;
    void GetLevelSizeInfo(const indexlibv2::framework::LevelInfo& levelInfo,
                          const std::map<segmentid_t, index_base::SegmentMergeInfo>& segMergeInfoMap,
                          std::vector<std::vector<size_t>>& segmentsSize, std::vector<size_t>& actualLevelSize);
    void GetLevelThreshold(uint32_t levelNum, size_t bottomLevelSize, std::vector<size_t>& levelsThreshold);

private:
    double mSpaceAmplification;

private:
    friend class ShardBasedMergeStrategyTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShardBasedMergeStrategy);
}} // namespace indexlib::merger
