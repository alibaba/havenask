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

#include <stdint.h>
#include <string>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class RealtimeMergeStrategy : public MergeStrategy
{
public:
    static const std::string MAX_SMALL_SEGMENT_COUNT;
    static const std::string DO_MERGE_SIZE_THRESHOLD;
    static const std::string DONT_MERGE_SIZE_THRESHOLD;

public:
    RealtimeMergeStrategy(const merger::SegmentDirectoryPtr& segDir = merger::SegmentDirectoryPtr(),
                          const config::IndexPartitionSchemaPtr& schema = config::IndexPartitionSchemaPtr());
    ~RealtimeMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(RealtimeMergeStrategy, REALTIME_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;

    std::string GetParameter() const override;

    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;

    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

public:
    void SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }

private:
    int32_t mMaxSmallSegmentCount;
    int64_t mDoMergeSizeThreshold;
    int64_t mDontMergeSizeThreshold;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimeMergeStrategy);
}} // namespace indexlib::merger
