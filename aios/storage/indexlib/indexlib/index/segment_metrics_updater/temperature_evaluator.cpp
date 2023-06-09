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
#include "indexlib/index/segment_metrics_updater/temperature_evaluator.h"

#include "indexlib/config/temperature_condition.h"
#include "indexlib/index/segment_metrics_updater/filter_matcher_creator.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TemperatureEvaluator);

TemperatureEvaluator::TemperatureEvaluator() {}

TemperatureEvaluator::~TemperatureEvaluator() {}

bool TemperatureEvaluator::Init(const config::AttributeSchemaPtr& attrSchema,
                                const config::TemperatureLayerConfigPtr& config)
{
    const std::vector<TemperatureConditionPtr>& conditions = config->GetConditions();
    for (size_t i = 0; i < conditions.size(); i++) {
        auto property = TemperatureSegmentMetricsUpdater::StringToTemperatureProperty(conditions[i]->GetProperty());
        const std::vector<ConditionFilterPtr>& filters = conditions[i]->GetFilters();
        int32_t filterNum = filters.size();
        mMatchInfos.insert(make_pair(i, make_pair(property, filterNum)));
        for (const auto& filter : filters) {
            string fieldName = filter->GetFieldName();
            auto iter = mMatchers.find(fieldName);
            if (iter == mMatchers.end()) {
                FilterMatcherPtr matcher(FilterMatcherCreator::Create(attrSchema, filter, i));
                if (!matcher) {
                    IE_LOG(ERROR, "filter matcher create failed");
                    return false;
                }
                mMatchers.insert(make_pair(fieldName, matcher));
            } else {
                if (!FilterMatcherCreator::AddValue(iter->second, filter, i)) {
                    IE_LOG(ERROR, "add filter value failed");
                    return false;
                }
            }
        }
    }
    mDefaultProperty = TemperatureSegmentMetricsUpdater::StringToTemperatureProperty(config->GetDefaultProperty());
    return true;
}

bool TemperatureEvaluator::InitForMerge(const config::AttributeSchemaPtr& attrSchema,
                                        const config::TemperatureLayerConfigPtr& config,
                                        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders)
{
    const std::vector<TemperatureConditionPtr>& conditions = config->GetConditions();
    for (size_t i = 0; i < conditions.size(); i++) {
        auto property = TemperatureSegmentMetricsUpdater::StringToTemperatureProperty(conditions[i]->GetProperty());
        const std::vector<ConditionFilterPtr>& filters = conditions[i]->GetFilters();
        int32_t filterNum = filters.size();
        mMatchInfos.insert(make_pair(i, make_pair(property, filterNum)));
        for (const auto& filter : filters) {
            string fieldName = filter->GetFieldName();
            auto iter = mMatchers.find(fieldName);
            if (iter == mMatchers.end()) {
                FilterMatcherPtr matcher(FilterMatcherCreator::Create(attrSchema, filter, attrReaders, i));
                if (!matcher) {
                    IE_LOG(ERROR, "filter matcher create failed");
                    return false;
                }
                mMatchers.insert(make_pair(fieldName, matcher));
            } else {
                if (!FilterMatcherCreator::AddValue(iter->second, filter, i)) {
                    IE_LOG(ERROR, "add filter value failed");
                    return false;
                }
            }
        }
    }
    mDefaultProperty = TemperatureSegmentMetricsUpdater::StringToTemperatureProperty(config->GetDefaultProperty());
    return true;
}

TemperatureProperty TemperatureEvaluator::Evaluate(const document::DocumentPtr& doc)
{
    mCurrentMatchInfo.clear();
    for (auto iter = mMatchers.begin(); iter != mMatchers.end(); iter++) {
        mMatchIds.clear();
        iter->second->Match(doc, mMatchIds);
        for (auto matchIdx : mMatchIds) {
            auto iter2 = mCurrentMatchInfo.find(matchIdx);
            if (iter2 == mCurrentMatchInfo.end()) {
                mCurrentMatchInfo.insert(make_pair(matchIdx, 1));
            } else {
                iter2->second++;
            }
        }
    }
    for (auto iter = mCurrentMatchInfo.begin(); iter != mCurrentMatchInfo.end(); iter++) {
        int32_t idx = iter->first;
        const pair<TemperatureProperty, int32_t>& expectInfo = mMatchInfos[idx];
        if (iter->second == expectInfo.second) {
            return expectInfo.first;
        }
    }
    return mDefaultProperty;
}

TemperatureProperty TemperatureEvaluator::Evaluate(segmentid_t oldSegId, docid_t oldLocalId)
{
    mCurrentMatchInfo.clear();
    for (auto iter = mMatchers.begin(); iter != mMatchers.end(); iter++) {
        mMatchIds.clear();
        iter->second->Match(oldSegId, oldLocalId, mMatchIds);
        for (auto matchIdx : mMatchIds) {
            auto iter2 = mCurrentMatchInfo.find(matchIdx);
            if (iter2 == mCurrentMatchInfo.end()) {
                mCurrentMatchInfo.insert(make_pair(matchIdx, 1));
            } else {
                iter2->second++;
            }
        }
    }
    for (auto iter = mCurrentMatchInfo.begin(); iter != mCurrentMatchInfo.end(); iter++) {
        int32_t idx = iter->first;
        const pair<TemperatureProperty, int32_t>& expectInfo = mMatchInfos[idx];
        if (iter->second == expectInfo.second) {
            return expectInfo.first;
        }
    }
    return mDefaultProperty;
}
}} // namespace indexlib::index
