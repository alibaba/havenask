#ifndef __INDEXLIB_DEFAULT_SEGMENT_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_DEFAULT_SEGMENT_ATTRIBUTE_UPDATER_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/segment_metrics_updater.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index/sort_value_evaluator.h"
#include <unordered_map>
#include <tr1/memory>

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(index, DocInfo);
DECLARE_REFERENCE_CLASS(index, Comparator);
DECLARE_REFERENCE_CLASS(index, Reference);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(partition, SortValueEvaluator);

IE_NAMESPACE_BEGIN(index);

class MaxMinSegmentMetricsUpdater : public index::SegmentMetricsUpdater
{
public:
    MaxMinSegmentMetricsUpdater();
    ~MaxMinSegmentMetricsUpdater();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const util::KeyValueMap& parameters) override;
    bool InitForMerge(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
        const util::KeyValueMap& parameters) override;

public:
    void Update(const document::DocumentPtr& doc) override;
    void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId) override;
    autil::legacy::json::JsonMap Dump() const override;

public:
    static const std::string UPDATER_NAME;
    
public:
    static std::string GetAttrKey(const std::string& attrName)
    {
        return ATTRIBUTE_IDENTIFIER + ":" + attrName;
    }

    template <typename T>
    static bool GetAttrValues(index_base::SegmentGroupMetricsPtr segmentGroupMetrics,
        const std::string& attrName, T& maxValue, T& minValue);

public:
    std::string GetAttrName() const { return mAttrName; }
    FieldType GetFieldType() const { return mFieldType; }
    uint32_t GetCheckedDocCount() const { return mCheckedDocCount; }

    template <typename T> bool GetAttrValues(T& maxValue, T& minValue) const;

private:
    template <typename T> void FillJsonMap(autil::legacy::json::JsonMap& jsonMap) const;
    void Evaluate(const document::DocumentPtr& document, index::DocInfo* docInfo);

    template <typename T>
    void FillReference(const index::AttributeSegmentReaderPtr& segReader, docid_t localId);

    void DoUpdate();
    bool ValidateFieldType(FieldType ft) const;

private:
    uint32_t mCheckedDocCount;
    index::DocInfo* mCurDocInfo;
    index::DocInfo* mMinDocInfo;
    index::DocInfo* mMaxDocInfo;
    std::string mAttrName;
    FieldType mFieldType;
    index::Reference* mReference;
    index::ComparatorPtr mComparator;
    index::DocInfoAllocator mDocInfoAllocator;
    SortValueEvaluatorPtr mEvaluator;
    index::OfflineAttributeSegmentReaderContainerPtr mReaderContainer;
    std::unordered_map<segmentid_t, index::AttributeSegmentReaderPtr> mSegReaderMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MaxMinSegmentMetricsUpdater);

///////////////////////

template <typename T>
bool MaxMinSegmentMetricsUpdater::GetAttrValues(
    index_base::SegmentGroupMetricsPtr segmentGroupMetrics, const std::string& attrName,
    T& maxValue, T& minValue)
{
    std::string attrKey = GetAttrKey(attrName);
    std::map<std::string, T> valueMap;
    if (!segmentGroupMetrics->Get(attrKey, valueMap))
    {
        return false;
    }
    auto iter = valueMap.find("max");
    if (iter == valueMap.end())
    {
        return false;
    }
    maxValue = iter->second;
    minValue = valueMap.at("min");
    return true;
}

template <typename T>
bool MaxMinSegmentMetricsUpdater::GetAttrValues(T& maxValue, T& minValue) const
{
    if (mCheckedDocCount == 0)
    {
        return false;
    }
    auto typedRefer = dynamic_cast<ReferenceTyped<T>*>(mReference);
    if (!typedRefer)
    {
        IE_LOG(ERROR, "ref type error, current field type is [%d]", mFieldType);
        return false;
    }
    typedRefer->Get(mMaxDocInfo, maxValue);
    typedRefer->Get(mMinDocInfo, minValue);
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFAULT_SEGMENT_METRICS_UPDATER_H
