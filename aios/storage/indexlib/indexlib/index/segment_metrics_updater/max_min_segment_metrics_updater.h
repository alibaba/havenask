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
#ifndef __INDEXLIB_DEFAULT_SEGMENT_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_DEFAULT_SEGMENT_ATTRIBUTE_UPDATER_H

#include <memory>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/sort_value_evaluator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(index::legacy, DocInfo);
DECLARE_REFERENCE_CLASS(index::legacy, Comparator);
DECLARE_REFERENCE_CLASS(index::legacy, Reference);
DECLARE_REFERENCE_CLASS(partition, SortValueEvaluator);

namespace indexlib { namespace index {

class MaxMinSegmentMetricsUpdater : public index::SegmentMetricsUpdater
{
public:
    MaxMinSegmentMetricsUpdater(util::MetricProviderPtr metricProvider);
    ~MaxMinSegmentMetricsUpdater();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              const util::KeyValueMap& parameters) override;
    bool InitForMerge(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const util::KeyValueMap& parameters) override;

public:
    void Update(const document::DocumentPtr& doc) override;
    void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue) override;
    autil::legacy::json::JsonMap Dump() const override;
    std::string GetUpdaterName() const override { return MaxMinSegmentMetricsUpdater::UPDATER_NAME; }

public:
    static const std::string UPDATER_NAME;

public:
    static std::string GetAttrKey(const std::string& attrName) { return ATTRIBUTE_IDENTIFIER + ":" + attrName; }

    template <typename T>
    static bool GetAttrValues(std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics,
                              const std::string& attrName, T& maxValue, T& minValue);

public:
    std::string GetAttrName() const { return mAttrName; }
    FieldType GetFieldType() const { return mFieldType; }
    uint32_t GetCheckedDocCount() const { return mCheckedDocCount; }

    template <typename T>
    bool GetAttrValues(T& maxValue, T& minValue) const;

private:
    template <typename T>
    void FillJsonMap(autil::legacy::json::JsonMap& jsonMap) const;
    void Evaluate(const document::DocumentPtr& document, index::legacy::DocInfo* docInfo);

    template <typename T>
    void FillReference(const index::AttributeSegmentReaderWithCtx& segReader, docid_t localId);

    void DoUpdate();
    bool ValidateFieldType(FieldType ft) const;

private:
    uint32_t mCheckedDocCount;
    index::legacy::DocInfo* mCurDocInfo;
    index::legacy::DocInfo* mMinDocInfo;
    index::legacy::DocInfo* mMaxDocInfo;
    std::string mAttrName;
    FieldType mFieldType;
    index::legacy::Reference* mReference;
    std::shared_ptr<index::legacy::Comparator> mComparator;
    index::legacy::DocInfoAllocator mDocInfoAllocator;
    SortValueEvaluatorPtr mEvaluator;
    index::OfflineAttributeSegmentReaderContainerPtr mReaderContainer;
    std::unordered_map<segmentid_t, index::AttributeSegmentReaderWithCtx> mSegReaderMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MaxMinSegmentMetricsUpdater);

///////////////////////

template <typename T>
bool MaxMinSegmentMetricsUpdater::GetAttrValues(std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics,
                                                const std::string& attrName, T& maxValue, T& minValue)
{
    std::string attrKey = GetAttrKey(attrName);
    std::map<std::string, T> valueMap;
    if (!segmentGroupMetrics->Get(attrKey, valueMap)) {
        return false;
    }
    auto iter = valueMap.find("max");
    if (iter == valueMap.end()) {
        return false;
    }
    maxValue = iter->second;
    minValue = valueMap.at("min");
    return true;
}

template <typename T>
bool MaxMinSegmentMetricsUpdater::GetAttrValues(T& maxValue, T& minValue) const
{
    if (mCheckedDocCount == 0) {
        return false;
    }
    auto typedRefer = dynamic_cast<index::legacy::ReferenceTyped<T>*>(mReference);
    if (!typedRefer) {
        IE_LOG(ERROR, "ref type error, current field type is [%d]", mFieldType);
        return false;
    }
    bool isNull1 = false, isNull2 = false;
    typedRefer->Get(mMaxDocInfo, maxValue, isNull1);
    typedRefer->Get(mMinDocInfo, minValue, isNull2);
    // TODO support null
    return true;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_DEFAULT_SEGMENT_METRICS_UPDATER_H
