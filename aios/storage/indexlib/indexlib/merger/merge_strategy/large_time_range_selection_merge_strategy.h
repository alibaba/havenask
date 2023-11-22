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

#include <algorithm>
#include <limits>
#include <ostream>
#include <stddef.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy_define.h"
#include "indexlib/merger/merge_strategy/time_series_segment_merge_info_creator.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

struct LargeTimeRangeSelectionMergeStrategyInputLimits {
public:
    LargeTimeRangeSelectionMergeStrategyInputLimits() {}
    bool FromString(const std::string& inputStr)
    {
        std::vector<std::string> kvPairs;
        autil::StringUtil::fromString(inputStr, kvPairs, ";");
        bool isSetAttributeField = false;
        for (size_t i = 0; i < kvPairs.size(); i++) {
            std::vector<std::string> keyValue;
            autil::StringUtil::fromString(kvPairs[i], keyValue, "=");
            if (keyValue.size() != 2) {
                return false;
            }
            if (keyValue[0] == "attribute-field") {
                attributeField = keyValue[1];
                isSetAttributeField = true;
                continue;
            }
            return false;
        }
        if (!isSetAttributeField) {
            IE_LOG(ERROR, "not set attribute field");
            return false;
        }
        return true;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        ss << "attribute-field=" << attributeField << ";";
        return ss.str();
    }

    std::string attributeField;

private:
    IE_LOG_DECLARE();
};

template <typename T>
class LargeTimeRangeSelectionMergeProcessorImpl : public TimeSeriesMergeProcessor
{
public:
    LargeTimeRangeSelectionMergeProcessorImpl(std::vector<TimeSeriesStrategyCondition<T>>& timeSeriesStrategyConditions,
                                              LargeTimeRangeSelectionMergeStrategyInputLimits& timeSeriesInputLimits)
        : mLargeTimeRangeSelectionStrategyConditions(timeSeriesStrategyConditions)
        , mLargeTimeRangeSelectionInputLimits(timeSeriesInputLimits)
    {
    }
    ~LargeTimeRangeSelectionMergeProcessorImpl() {}

public:
    void CreateMergePlans(const index_base::SegmentMergeInfos& segMergeInfos, std::vector<MergePlan>& plans) override
    {
        T maxSortFieldValue;
        T maxBaseline;
        auto timeSeriesSegmentMergeInfos = TimeSeriesSegmentMergeInfoCreator::CreateTimeSeriesSegmentMergeInfo<T>(
            segMergeInfos, mLargeTimeRangeSelectionStrategyConditions,
            mLargeTimeRangeSelectionInputLimits.attributeField, maxSortFieldValue, maxBaseline);
        MergeTask task;
        size_t idx = 0;
        while (idx < timeSeriesSegmentMergeInfos.size()) {
            const TimeSeriesSegmentMergeInfo<T>& mergeInfo = timeSeriesSegmentMergeInfos[idx];
            T expectInterval = std::numeric_limits<T>::max();
            if (GetInterval(mergeInfo, expectInterval)) {
                if (expectInterval < mergeInfo.mSortFieldRange) {
                    MergePlan plan;
                    plan.AddSegment(mergeInfo.mSegmentMergeInfo);
                    plans.push_back(plan);
                }
            }
            ++idx;
        }
    }

    std::string GetParameter() const override
    {
        std::stringstream ss;
        for (const auto& timeSeriesStrategyCondition : mLargeTimeRangeSelectionStrategyConditions) {
            ss << timeSeriesStrategyCondition.ToString() << ";";
        }
        return ss.str();
    }

private:
    bool GetInterval(const TimeSeriesSegmentMergeInfo<T>& timeSeriesSegmentMergeInfo, T& interval)
    {
        for (auto& strategyCondition : mLargeTimeRangeSelectionStrategyConditions) {
            bool matchConditon = true;
            for (auto conditionIter = strategyCondition.conditions.begin();
                 conditionIter != strategyCondition.conditions.end(); ++conditionIter) {
                auto fieldValueIter = timeSeriesSegmentMergeInfo.mFieldValues.find(conditionIter->first);
                if (fieldValueIter == timeSeriesSegmentMergeInfo.mFieldValues.end() ||
                    !conditionIter->second->Compare(fieldValueIter->second)) {
                    matchConditon = false;
                    break;
                }
            }
            if (matchConditon) {
                interval = strategyCondition.inputInterval;
                return true;
            }
        }
        return false;
    }

private:
    std::vector<TimeSeriesStrategyCondition<T>> mLargeTimeRangeSelectionStrategyConditions;
    LargeTimeRangeSelectionMergeStrategyInputLimits mLargeTimeRangeSelectionInputLimits;
};

class LargeTimeRangeSelectionMergeStrategy : public MergeStrategy
{
public:
    LargeTimeRangeSelectionMergeStrategy(const SegmentDirectoryPtr& segDir,
                                         const config::IndexPartitionSchemaPtr& schema);
    ~LargeTimeRangeSelectionMergeStrategy();

public:
    void SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }

    std::string GetParameter() const override;

    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;

    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

    void SetParameter(const config::MergeStrategyParameter& param) override;

private:
    TimeSeriesMergeProcessorPtr mLargeTimeRangeMergeProcessor;
    LargeTimeRangeSelectionMergeStrategyInputLimits mLargeTimeRangeSelectionInputLimits;
    FieldType mFieldType;

public:
    DECLARE_MERGE_STRATEGY_CREATOR(LargeTimeRangeSelectionMergeStrategy, LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_STR);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LargeTimeRangeSelectionMergeStrategy);
}} // namespace indexlib::merger
