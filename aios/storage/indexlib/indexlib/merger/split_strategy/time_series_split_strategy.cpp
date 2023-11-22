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
#include "indexlib/merger/split_strategy/time_series_split_strategy.h"

#include <algorithm>
#include <cstddef>

#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/util/IoExceptionController.h"

using namespace std;

namespace indexlib { namespace merger {
using namespace indexlib::util;
using namespace indexlib::config;
IE_LOG_SETUP(merger, TimeSeriesSplitStrategy);
IE_LOG_SETUP_TEMPLATE(merger, TimeSeriesSplitProcessorImpl);

const std::string TimeSeriesSplitStrategy::STRATEGY_NAME = "time_series";

TimeSeriesSplitStrategy::TimeSeriesSplitStrategy(index::SegmentDirectoryBasePtr segDir,
                                                 config::IndexPartitionSchemaPtr schema,
                                                 index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                                                 const index_base::SegmentMergeInfos& segMergeInfos,
                                                 const MergePlan& plan,
                                                 std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                                                 const util::MetricProviderPtr& metrics)
    : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan, hintDocInfos, metrics)
    , mTimeSeriesSplitProcessor(NULL)
{
}

TimeSeriesSplitStrategy::~TimeSeriesSplitStrategy()
{
    mReaderCache.clear();
    if (mTimeSeriesSplitProcessor) {
        delete mTimeSeriesSplitProcessor;
        mTimeSeriesSplitProcessor = NULL;
    }
}

bool TimeSeriesSplitStrategy::Init(const util::KeyValueMap& parameters)
{
    mAttributeName = GetValueFromKeyValueMap(parameters, "attribute", "");
    string rangesStr = GetValueFromKeyValueMap(parameters, "ranges", "");

    // not success ?
    auto attributeConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(mAttributeName);

    if (!attributeConfig) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid field [%s] for time series split strategy", mAttributeName.c_str());
        return false;
    }
    mFieldType = attributeConfig->GetFieldType();

#define CREATE_PROCESSOR(fieldType)                                                                                    \
    case fieldType:                                                                                                    \
        mTimeSeriesSplitProcessor = new TimeSeriesSplitProcessorImpl<FieldTypeTraits<fieldType>::AttrItemType>();      \
        mTimeSeriesSplitProcessor->Init(mAttributeName, rangesStr, mSegMergeInfos);                                    \
        break;

    switch (mFieldType) {
        NUMBER_FIELD_MACRO_HELPER(CREATE_PROCESSOR);
    default:
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mFieldType);
        return false;
    }
#undef CREATE_PROCESSOR

    return true;
}

segmentindex_t TimeSeriesSplitStrategy::DoProcess(segmentid_t oldSegId, docid_t oldLocalId, int64_t& hintValue)
{
    if (mReaderContainer && mTimeSeriesSplitProcessor) {
        if (mReaderCache.find(make_pair(mAttributeName, oldSegId)) == mReaderCache.end()) {
            auto attributeSegmentReader = mReaderContainer->GetAttributeSegmentReader(mAttributeName, oldSegId);
            mReaderCache[make_pair(mAttributeName, oldSegId)] = {attributeSegmentReader,
                                                                 attributeSegmentReader->CreateReadContextPtr(&mPool)};
        }
        return mTimeSeriesSplitProcessor->GetDocSegment(mReaderCache[make_pair(mAttributeName, oldSegId)], oldLocalId);
    } else {
        INDEXLIB_FATAL_ERROR(Runtime, "get segment [%d] attribute reader failed", oldSegId);
        return 0;
    }
    return 0;
}
}} // namespace indexlib::merger
