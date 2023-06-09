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

#include "autil/Log.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
namespace indexlibv2::framework {
struct SegmentTopologyInfo;
class LevelInfo;
class TabletData;

} // namespace indexlibv2::framework

namespace indexlibv2::table {

class KeyValueOptimizeMergeStrategy : public MergeStrategy
{
public:
    KeyValueOptimizeMergeStrategy() = default;
    ~KeyValueOptimizeMergeStrategy() = default;

    std::string GetName() const override { return MergeStrategyDefine::OPTIMIZE_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    void FillSrcSegments(const std::shared_ptr<framework::LevelInfo>& levelInfo, std::vector<SegmentMergePlan>& plans);
    void FillTargetSegments(segmentid_t baseId, const std::shared_ptr<framework::TabletData>& tabletData,
                            std::vector<SegmentMergePlan>& plans);
    void FillTopologyInfo(const std::vector<SegmentMergePlan>& plans,
                          const std::shared_ptr<framework::LevelInfo>& levelInfo,
                          std::map<segmentid_t, framework::SegmentTopologyInfo>& topoInfos);
    void FillMergePlan(const framework::IndexTaskContext* context, const std::vector<SegmentMergePlan>& plans,
                       const std::shared_ptr<framework::LevelInfo>& levelInfo,
                       const std::map<segmentid_t, framework::SegmentTopologyInfo>& topoInfos,
                       std::shared_ptr<MergePlan>& mergePlan);

    bool CheckLevelInfo(const std::shared_ptr<framework::LevelInfo>& levelInfo) const;
    void CollectSegmentDescriptions(const std::shared_ptr<framework::LevelInfo>& originalLevelInfo,
                                    const std::map<segmentid_t, framework::SegmentTopologyInfo>& topoInfos,
                                    const std::shared_ptr<MergePlan>& mergePlan, framework::Version& targetVersion);
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
