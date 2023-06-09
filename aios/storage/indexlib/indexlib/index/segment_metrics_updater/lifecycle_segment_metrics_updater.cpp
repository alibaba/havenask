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
#include "indexlib/index/segment_metrics_updater/lifecycle_segment_metrics_updater.h"

#include <functional>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, LifecycleSegmentMetricsUpdater);

const string LifecycleSegmentMetricsUpdater::UPDATER_NAME = "lifecycle";
const string LifecycleSegmentMetricsUpdater::LIFECYCLE_PARAM = "lifecycle_param";
const string LifecycleSegmentMetricsUpdater::DEFAULT_LIFECYCLE_TAG = "default_lifecycle_tag";

LifecycleSegmentMetricsUpdater::~LifecycleSegmentMetricsUpdater() {}

template <typename T>
std::string LifecycleSegmentMetricsUpdater::GenerateLifeCycleTag() const
{
    T maxValue = std::numeric_limits<T>::min();
    T minValue = std::numeric_limits<T>::max();
    size_t checkedSegment = 0;
    for (const auto& segMergeInfo : mSegMergeInfos) {
        const auto& segMetrics = segMergeInfo.segmentMetrics;
        T curMax = T();
        T curMin = T();
        if (GetSegmentAttrValue(segMetrics, curMax, curMin)) {
            maxValue = std::max(maxValue, curMax);
            minValue = std::min(minValue, curMin);
            ++checkedSegment;
        }
    }
    if (checkedSegment == 0 || mThresholdInfos.IsEmpty() || mMaxMinUpdater.GetCheckedDocCount() == 0) {
        return mDefaultLifecycleTag;
    }
    T myMax = T();
    T myMin = T();
    if (!mMaxMinUpdater.GetAttrValues(myMax, myMin)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "get max and min value from MaxMinUpdater failed");
    }
    auto distance = maxValue - myMax;
    const auto& thresholdInfos = AnyCast<vector<pair<T, string>>>(mThresholdInfos);
    auto iter = thresholdInfos.begin();
    for (; iter != thresholdInfos.end(); ++iter) {
        if (distance < iter->first) {
            break;
        }
    }
    if (iter == thresholdInfos.begin()) {
        return mDefaultLifecycleTag;
    }
    return (iter - 1)->second;
}

template <typename T>
bool LifecycleSegmentMetricsUpdater::InitThresholdInfos()
{
    vector<pair<T, string>> thresholdInfos;
    StringTokenizer st(mLifecycleParam, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    T lastThreshold = T();
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer innerSt(st[i], ":", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (innerSt.getNumTokens() != 2) {
            IE_LOG(ERROR, "Invalid parameter for lifecycle param: [%s]", mLifecycleParam.c_str());
            return false;
        }
        T threshold = T();
        if (!StringUtil::fromString(innerSt[0], threshold)) {
            IE_LOG(ERROR, "Invalid parameter for lifecycle param: [%s]", mLifecycleParam.c_str());
            return false;
        }
        if (i != 0 && threshold <= lastThreshold) {
            IE_LOG(ERROR, "Invalid parameter for lifecycle param: [%s]", mLifecycleParam.c_str());
            return false;
        }
        lastThreshold = threshold;
        const string& tagName = innerSt[1];
        thresholdInfos.push_back({threshold, tagName});
    }
    mThresholdInfos = thresholdInfos;
    return true;
}

bool LifecycleSegmentMetricsUpdater::Init(const config::IndexPartitionSchemaPtr& schema,
                                          const config::IndexPartitionOptions& options,
                                          const util::KeyValueMap& parameters)
{
    if (!mMaxMinUpdater.Init(schema, options, parameters)) {
        IE_LOG(ERROR, "init [%s] updater failed", UPDATER_NAME.c_str());
        return false;
    }
    mLifecycleParam = GetValueFromKeyValueMap(parameters, LIFECYCLE_PARAM);
    mDefaultLifecycleTag = GetValueFromKeyValueMap(parameters, DEFAULT_LIFECYCLE_TAG);
    return true;
}

bool LifecycleSegmentMetricsUpdater::InitForMerge(const config::IndexPartitionSchemaPtr& schema,
                                                  const config::IndexPartitionOptions& options,
                                                  const index_base::SegmentMergeInfos& segMergeInfos,
                                                  const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                                  const util::KeyValueMap& parameters)
{
    if (!mMaxMinUpdater.InitForMerge(schema, options, segMergeInfos, attrReaders, parameters)) {
        IE_LOG(ERROR, "init [%s] updater for merge failed", UPDATER_NAME.c_str());
        return false;
    }
    mIsMerge = true;
    mLifecycleParam = GetValueFromKeyValueMap(parameters, LIFECYCLE_PARAM);
    mDefaultLifecycleTag = GetValueFromKeyValueMap(parameters, DEFAULT_LIFECYCLE_TAG);
    mSegMergeInfos = segMergeInfos;

#define INIT_THRESHOLD_INFOS(fieldType)                                                                                \
    case fieldType:                                                                                                    \
        if (!InitThresholdInfos<FieldTypeTraits<fieldType>::AttrItemType>()) {                                         \
            return false;                                                                                              \
        }                                                                                                              \
        break;

    switch (mMaxMinUpdater.GetFieldType()) {
        NUMBER_FIELD_MACRO_HELPER(INIT_THRESHOLD_INFOS);
    default:
        assert(false);
        IE_LOG(ERROR, "invalid field type [%d]", mMaxMinUpdater.GetFieldType());
        return false;
    }
#undef INIT_THRESHOLD_INFOS

    return true;
}

void LifecycleSegmentMetricsUpdater::Update(const document::DocumentPtr& doc) { mMaxMinUpdater.Update(doc); }

void LifecycleSegmentMetricsUpdater::UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue)
{
    mMaxMinUpdater.UpdateForMerge(oldSegId, oldLocalId, hintValue);
}

template <typename T>
bool LifecycleSegmentMetricsUpdater::GetSegmentAttrValue(const indexlib::framework::SegmentMetrics& metrics,
                                                         T& maxValue, T& minValue) const
{
    auto groupMetrics = metrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    if (!groupMetrics) {
        IE_LOG(WARN, "no customize group found in segment metrics");
        return false;
    }
    size_t checkedCount = 0;
    if (!groupMetrics->Get<size_t>(CHECKED_DOC_COUNT, checkedCount)) {
        IE_LOG(WARN, "no [%s] found in segment metrics", CHECKED_DOC_COUNT.c_str());
        return false;
    }
    if (checkedCount == 0) {
        return false;
    }

    string attrKey = GetAttrKey(mMaxMinUpdater.GetAttrName());
    json::JsonMap attrValues;
    if (!groupMetrics->Get<json::JsonMap>(attrKey, attrValues)) {
        IE_LOG(WARN, "no [%s] found in segment metrics", attrKey.c_str());
        return false;
    }

    // if "min" or "max" key not exist, an std exception is happened
    maxValue = json::JsonNumberCast<T>(attrValues["max"]);
    minValue = json::JsonNumberCast<T>(attrValues["min"]);
    return true;
}

json::JsonMap LifecycleSegmentMetricsUpdater::Dump() const
{
    auto jsonMap = mMaxMinUpdater.Dump();
#define GENERATE_LIFECYCLE_TAG(ft)                                                                                     \
    case ft:                                                                                                           \
        jsonMap[LIFECYCLE] = GenerateLifeCycleTag<FieldTypeTraits<ft>::AttrItemType>();                                \
        break;

    switch (mMaxMinUpdater.GetFieldType()) {
        NUMBER_FIELD_MACRO_HELPER(GENERATE_LIFECYCLE_TAG);
    default:
        assert(false);
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mMaxMinUpdater.GetFieldType());
    }

#undef GENERATE_LIFECYCLE_TAG

    return jsonMap;
}
}} // namespace indexlib::index
