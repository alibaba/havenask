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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentTopologyInfo.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"

namespace indexlibv2 { namespace table {

class ShardBasedMergeStrategy : public MergeStrategy
{
public:
    ShardBasedMergeStrategy() = default;
    ~ShardBasedMergeStrategy() = default;

public:
    std::string GetName() const override { return MergeStrategyDefine::SHARD_BASED_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    void SetParameter(const config::MergeStrategyParameter& param);
    void GetLevelThreshold(uint32_t levelNum, size_t bottomLevelSize, std::vector<size_t>& levelsThreshold);
    Status GetLevelSizeInfo(const std::shared_ptr<indexlibv2::framework::LevelInfo>& levelInfo,
                            const std::shared_ptr<framework::TabletData>& tabletData,
                            std::vector<std::vector<size_t>>& segmentsSize, std::vector<size_t>& actualLevelSize);
    std::pair<Status, std::shared_ptr<MergePlan>> DoCreateMergePlan(const framework::IndexTaskContext* context,
                                                                    const std::vector<std::vector<bool>>& mergeTag);
    void CollectSegmentDescriptions(const std::shared_ptr<indexlibv2::framework::LevelInfo>& originalLevelInfo,
                                    const std::map<segmentid_t, indexlibv2::framework::SegmentTopologyInfo>& topoInfos,
                                    const std::shared_ptr<MergePlan>& mergePlan, framework::Version& targetVersion);
    std::pair<Status, std::vector<std::vector<bool>>>
    GenerateMergeTag(const std::shared_ptr<indexlibv2::framework::LevelInfo>& levelInfo,
                     const std::shared_ptr<framework::TabletData>& tabletData);

private:
    static double CalculateSpaceAmplification(uint32_t percent, uint32_t levelNum);

public:
    static uint32_t CalculateLevelPercent(double spaceAmplification, uint32_t levelNum);

public:
    void TEST_SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }

private:
    double _spaceAmplification = 1.5;
    bool _disableLastLevelMerge = false;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
