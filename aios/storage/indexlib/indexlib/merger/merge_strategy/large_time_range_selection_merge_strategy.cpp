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
#include "indexlib/merger/merge_strategy/large_time_range_selection_merge_strategy.h"

#include <map>
#include <memory>

#include "autil/CommonMacros.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::config;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, LargeTimeRangeSelectionMergeStrategy);
IE_LOG_SETUP(merger, LargeTimeRangeSelectionMergeStrategyInputLimits);

LargeTimeRangeSelectionMergeStrategy::LargeTimeRangeSelectionMergeStrategy(
    const merger::SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
{
}

LargeTimeRangeSelectionMergeStrategy::~LargeTimeRangeSelectionMergeStrategy() {}

void LargeTimeRangeSelectionMergeStrategy::SetParameter(const config::MergeStrategyParameter& param)
{
    if (unlikely(!mLargeTimeRangeSelectionInputLimits.FromString(param.inputLimitParam))) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merger stargey [%s], input args",
                             LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_STR);
        return;
    }
    vector<string> conditions;
    autil::StringUtil::fromString(param.strategyConditions, conditions, ";");
    std::string sortField = mLargeTimeRangeSelectionInputLimits.attributeField;
    auto attributeConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(sortField);
    if (unlikely(!attributeConfig)) {
        INDEXLIB_FATAL_ERROR(Runtime, "can not find attr [%s] config", sortField.c_str());
        return;
    }
    mFieldType = attributeConfig->GetFieldType();
#define CREATE_PROCESSOR(fieldType)                                                                                    \
    case fieldType: {                                                                                                  \
        std::vector<TimeSeriesStrategyCondition<FieldTypeTraits<fieldType>::AttrItemType>>                             \
            timeSeriesStrategyConditions;                                                                              \
        timeSeriesStrategyConditions.resize(conditions.size());                                                        \
        for (size_t i = 0; i < conditions.size(); ++i) {                                                               \
            if (!timeSeriesStrategyConditions[i].FromString(conditions[i])) {                                          \
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy [%s] condition [%s]",         \
                                     LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_STR, conditions[i].c_str());            \
                return;                                                                                                \
            }                                                                                                          \
        }                                                                                                              \
        mLargeTimeRangeMergeProcessor.reset(                                                                           \
            new LargeTimeRangeSelectionMergeProcessorImpl<FieldTypeTraits<fieldType>::AttrItemType>(                   \
                timeSeriesStrategyConditions, mLargeTimeRangeSelectionInputLimits));                                   \
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

string LargeTimeRangeSelectionMergeStrategy::GetParameter() const
{
    if (mLargeTimeRangeMergeProcessor) {
        stringstream ss;
        ss << "inputLimit=" << mLargeTimeRangeSelectionInputLimits.ToString() << ";"
           << "strategyCondition=" << mLargeTimeRangeMergeProcessor->GetParameter();
        return ss.str();
    } else {
        IE_LOG(ERROR, "LargeTimeRangeMergeProcessor is null");
        return "";
    }
}

MergeTask
LargeTimeRangeSelectionMergeStrategy::CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                                                 const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}

MergeTask LargeTimeRangeSelectionMergeStrategy::CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                                                                const indexlibv2::framework::LevelInfo& levelInfo)
{
    MergeTask task;
    std::vector<MergePlan> plans;
    if (unlikely(mLargeTimeRangeMergeProcessor == NULL)) {
        IE_LOG(ERROR, "MergeProcessor is null");
        return task;
    }
    mLargeTimeRangeMergeProcessor->CreateMergePlans(segMergeInfos, plans);
    for (size_t i = 0; i < plans.size(); i++) {
        IE_LOG(INFO, "add merge plan [%s]", plans[i].ToString().c_str());
        task.AddPlan(plans[i]);
    }
    return task;
}
}} // namespace indexlib::merger
