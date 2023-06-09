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
#include "indexlib/index/segment_metrics_updater/time_series_segment_metrics_updater.h"

#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace index {

IE_LOG_SETUP(index, TimeSeriesSegmentMetricsUpdater);
IE_LOG_SETUP_TEMPLATE(merger, TimeSeriesSegmentMetricsProcessorImpl);

const string TimeSeriesSegmentMetricsUpdater::UPDATER_NAME = "time_series";
const string TimeSeriesSegmentMetricsUpdater::PERIOD_PARAM = "period";
const string TimeSeriesSegmentMetricsUpdater::MIN_DOC_COUNT_PARAM = "min_doc_count";
const string TimeSeriesSegmentMetricsUpdater::CONTINUOUS_LEN_PARAM = "continuous_len";
const string TimeSeriesSegmentMetricsUpdater::DEFAULT_PERIOD = "1";
const string TimeSeriesSegmentMetricsUpdater::DEFAULT_MIN_DOC_COUNT = "10000";
const string TimeSeriesSegmentMetricsUpdater::DEFAULT_CONTINUOUS_LEN = "2";

TimeSeriesSegmentMetricsUpdater::TimeSeriesSegmentMetricsUpdater(util::MetricProviderPtr metricProvider)
    : SegmentMetricsUpdater(metricProvider)
{
}

TimeSeriesSegmentMetricsUpdater::~TimeSeriesSegmentMetricsUpdater()
{
    mSegReaderMap.clear();
    if (mProcessor) {
        delete mProcessor;
        mProcessor = nullptr;
    }
}

bool TimeSeriesSegmentMetricsUpdater::Init(const config::IndexPartitionSchemaPtr& schema,
                                           const config::IndexPartitionOptions& options,
                                           const util::KeyValueMap& parameters)
{
    auto iterator = parameters.find("attribute_name");
    if (iterator == parameters.end()) {
        IE_LOG(ERROR, "must set attribute_name");
        return false;
    }

    auto attrName = iterator->second;
    auto periodStr = GetValueFromKeyValueMap(parameters, PERIOD_PARAM, DEFAULT_PERIOD);
    auto minDocCountStr = GetValueFromKeyValueMap(parameters, MIN_DOC_COUNT_PARAM, DEFAULT_MIN_DOC_COUNT);
    auto continuousLenStr = GetValueFromKeyValueMap(parameters, CONTINUOUS_LEN_PARAM, DEFAULT_CONTINUOUS_LEN);
    size_t period, minDocCount, continuousLen;
#define GET_CONFIG_NUMBER(strVar, numVar)                                                                              \
    if (!StringUtil::fromString(strVar, numVar)) {                                                                     \
        IE_LOG(ERROR, "invalid #strVar config: %s", strVar.c_str());                                                   \
        return false;                                                                                                  \
    }

    GET_CONFIG_NUMBER(periodStr, period);
    GET_CONFIG_NUMBER(minDocCountStr, minDocCount);
    GET_CONFIG_NUMBER(continuousLenStr, continuousLen);
#undef GET_CONFIG_NUMBER
    if (period == 0) {
        INDEXLIB_FATAL_ERROR(BadParameter, "period config can not be 0");
    }

    auto attributeConfig = schema->GetAttributeSchema()->GetAttributeConfig(attrName);
    if (!attributeConfig) {
        IE_LOG(ERROR, "can not find attr [%s]", attrName.c_str());
        return false;
    }

    auto fieldType = attributeConfig->GetFieldType();
#define CREATE_PROCESSOR(fieldType)                                                                                    \
    case fieldType:                                                                                                    \
        mProcessor = new TimeSeriesSegmentMetricsProcessorImpl<FieldTypeTraits<fieldType>::AttrItemType>(              \
            attrName, period, minDocCount, continuousLen);                                                             \
        if (!mProcessor->Init(fieldType, attributeConfig)) {                                                           \
            delete mProcessor;                                                                                         \
            mProcessor = nullptr;                                                                                      \
            IE_LOG(ERROR, "init updater processor failed");                                                            \
            return false;                                                                                              \
        }                                                                                                              \
        break;

    switch (fieldType) {
        NUMBER_FIELD_MACRO_HELPER(CREATE_PROCESSOR);
    default:
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d] for updater", fieldType);
        return false;
    }
#undef CREATE_PROCESSOR
    return true;
}

bool TimeSeriesSegmentMetricsUpdater::InitForMerge(const config::IndexPartitionSchemaPtr& schema,
                                                   const config::IndexPartitionOptions& options,
                                                   const index_base::SegmentMergeInfos& segMergeInfos,
                                                   const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                                   const util::KeyValueMap& parameters)
{
    mReaderContainer = attrReaders;
    return Init(schema, options, parameters);
}

void TimeSeriesSegmentMetricsUpdater::Update(const document::DocumentPtr& doc) { mProcessor->Process(doc); }

void TimeSeriesSegmentMetricsUpdater::UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue)
{
    auto iter = mSegReaderMap.find(oldSegId);
    if (iter == mSegReaderMap.end()) {
        const auto& attrName = mProcessor->GetAttrName();
        auto segReader = mReaderContainer->GetAttributeSegmentReader(attrName, oldSegId);
        if (!segReader) {
            INDEXLIB_FATAL_ERROR(Runtime, "create segment reader for attr[%s] segid[%d] failed", attrName.c_str(),
                                 oldSegId);
        }
        iter = mSegReaderMap.insert(iter, {oldSegId, {segReader, segReader->CreateReadContextPtr(&mPool)}});
    }
    auto segReaderPair = iter->second;
    mProcessor->ProcessForMerge(segReaderPair, oldLocalId);
}

json::JsonMap TimeSeriesSegmentMetricsUpdater::Dump() const
{
    if (mProcessor) {
        return mProcessor->Dump();
    } else {
        return json::JsonMap();
    }
}

template <typename T>
T TimeSeriesSegmentMetricsProcessorImpl<T>::GetSubKey(T key)
{
    return key / mPeriod;
}

template <>
float TimeSeriesSegmentMetricsProcessorImpl<float>::GetSubKey(float key)
{
    return (int64_t)key / mPeriod;
}

template <>
double TimeSeriesSegmentMetricsProcessorImpl<double>::GetSubKey(double key)
{
    return (int64_t)key / mPeriod;
}
}} // namespace indexlib::index
