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
#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/merge_strategy/time_series_segment_merge_info_creator.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TimeSeriesMergeStrategy);

TimeSeriesMergeStrategy::TimeSeriesMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                 const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
    , mTimeSeriesMergeProcessor(NULL)
{
}

TimeSeriesMergeStrategy::~TimeSeriesMergeStrategy()
{
    if (mTimeSeriesMergeProcessor != NULL) {
        delete mTimeSeriesMergeProcessor;
        mTimeSeriesMergeProcessor = NULL;
    }
}

void TimeSeriesMergeStrategy::SetParameter(const config::MergeStrategyParameter& param)
{
    if (unlikely(!mTimeSeriesInputLimits.FromString(param.inputLimitParam))) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merger strategy [%s], input args",
                             TIMESERIES_MERGE_STRATEGY_STR);
        return;
    }
    if (unlikely(!mTimeSeriesOutputLimits.FromString(param.outputLimitParam))) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merger strategy [%s], output args",
                             TIMESERIES_MERGE_STRATEGY_STR);
        return;
    }
    vector<string> conditions;
    autil::StringUtil::fromString(param.strategyConditions, conditions, ";");
    std::string sortField = mTimeSeriesInputLimits.sortField;
    auto attributeConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(sortField);
    if (unlikely(!attributeConfig)) {
        INDEXLIB_FATAL_ERROR(Runtime, "can not find attr [%s] config", sortField.c_str());
        return;
    }
    mFieldType = attributeConfig->GetFieldType();

    if (attributeConfig->SupportNull()) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "Invalid parameter for merger strategy [%s], "
                             "sort-field [%s] can not enable null.",
                             TIMESERIES_MERGE_STRATEGY_STR, sortField.c_str());
    }

#define CREATE_PROCESSOR(fieldType)                                                                                    \
    case fieldType: {                                                                                                  \
        std::vector<TimeSeriesStrategyCondition<FieldTypeTraits<fieldType>::AttrItemType>>                             \
            timeSeriesStrategyConditions;                                                                              \
        timeSeriesStrategyConditions.resize(conditions.size());                                                        \
        for (size_t i = 0; i < conditions.size(); ++i) {                                                               \
            if (!timeSeriesStrategyConditions[i].FromString(conditions[i])) {                                          \
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merger strategy [%s], condition [%s]",       \
                                     TIMESERIES_MERGE_STRATEGY_STR, conditions[i].c_str());                            \
            }                                                                                                          \
        }                                                                                                              \
        mTimeSeriesMergeProcessor = new TimeSeriesMergeProcessorImpl<FieldTypeTraits<fieldType>::AttrItemType>(        \
            timeSeriesStrategyConditions, mTimeSeriesInputLimits, mTimeSeriesOutputLimits);                            \
        break;                                                                                                         \
    }
    switch (mFieldType) {
        NUMBER_FIELD_MACRO_HELPER(CREATE_PROCESSOR);
    default:
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mFieldType);
        return;
    }
#undef CREATE_PROCESSOR
}

string TimeSeriesMergeStrategy::GetParameter() const
{
    if (mTimeSeriesMergeProcessor) {
        stringstream ss;
        ss << "inputLimit=" << mTimeSeriesInputLimits.ToString() << ";"
           << "outputLimit=" << mTimeSeriesOutputLimits.ToString() << ";"
           << "strategyCondition=" << mTimeSeriesMergeProcessor->GetParameter();
        return ss.str();
    } else {
        IE_LOG(ERROR, "timeSeriesMergeProcessor is null");
        return "";
    }
}

MergeTask TimeSeriesMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                   const indexlibv2::framework::LevelInfo& levelInfo)
{
    MergeTask task;
    std::vector<MergePlan> plans;
    if (unlikely(mTimeSeriesMergeProcessor == NULL)) {
        IE_LOG(ERROR, "mTimeSeriesMergeProcessor is null");
        return task;
    }
    mTimeSeriesMergeProcessor->CreateMergePlans(segMergeInfos, plans);
    if (!NeedMerge(plans, segMergeInfos)) {
        return task;
    }
    for (size_t i = 0; i < plans.size(); i++) {
        IE_LOG(INFO, "add merge plan [%s]", plans[i].ToString().c_str());
        task.AddPlan(plans[i]);
    }
    return task;
}

MergeTask TimeSeriesMergeStrategy::CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                                              const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}

bool TimeSeriesMergeStrategy::NeedMerge(const vector<MergePlan>& plans, const SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < plans.size(); ++i) {
        if (plans[i].GetSegmentCount() > 1) {
            return true;
        }
    }
    return false;
}
}} // namespace indexlib::merger
