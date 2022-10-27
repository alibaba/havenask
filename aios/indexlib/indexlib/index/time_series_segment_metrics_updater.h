#ifndef __INDEXLIB_TIME_SERIES_SEGMENT_METRICS_UPDATER_H
#define __INDEXLIB_TIME_SERIES_SEGMENT_METRICS_UPDATER_H

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/segment_metrics_updater.h"
#include "indexlib/index/sort_value_evaluator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/util/key_value_map.h"
#include <autil/StringUtil.h>
#include <tr1/memory>
#include <unordered_map>

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(config, FieldConfig);

IE_NAMESPACE_BEGIN(index);

class TimeSeriesSegmentMetricsProcessor
{
public:
    TimeSeriesSegmentMetricsProcessor() {};
    virtual ~TimeSeriesSegmentMetricsProcessor() {};

public:
    virtual const std::string& GetAttrName() = 0;

public:
    virtual bool Init(FieldType fieldType, const config::FieldConfigPtr& fieldConfig) = 0;
    virtual void Process(const document::DocumentPtr& doc) = 0;
    virtual void ProcessForMerge(const AttributeSegmentReaderPtr& segReader, docid_t localId) = 0;
    virtual autil::legacy::json::JsonMap Dump() const = 0;
};

class TimeSeriesSegmentMetricsUpdater : public index::SegmentMetricsUpdater
{
public:
    TimeSeriesSegmentMetricsUpdater();
    ~TimeSeriesSegmentMetricsUpdater();

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
    static const std::string PERIOD_PARAM;
    static const std::string MIN_DOC_COUNT_PARAM;
    static const std::string CONTINUOUS_LEN_PARAM;

private:
    static const std::string DEFAULT_PERIOD;
    static const std::string DEFAULT_MIN_DOC_COUNT;
    static const std::string DEFAULT_CONTINUOUS_LEN;

public:
    static std::string GetAttrKey(const std::string& attrName)
    {
        return BASELINE_IDENTIFIER + ":" + attrName;
    }

    template <typename T>
    static bool GetBaseline(index_base::SegmentGroupMetricsPtr segmentGroupMetrics,
        const std::string& attrName, T& baseline)
    {
        return segmentGroupMetrics->Get(GetAttrKey(attrName), baseline);
    }

private:
    TimeSeriesSegmentMetricsProcessor* mProcessor;
    index::OfflineAttributeSegmentReaderContainerPtr mReaderContainer;
    std::unordered_map<segmentid_t, index::AttributeSegmentReaderPtr> mSegReaderMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimeSeriesSegmentMetricsUpdater);

template <typename T>
class TimeSeriesSegmentMetricsProcessorImpl : public TimeSeriesSegmentMetricsProcessor
{

public:
    TimeSeriesSegmentMetricsProcessorImpl(
        std::string& attrName, size_t period, size_t minDocCount, size_t continuousLen)
        : mAttrName(attrName)
        , mPeriod(period)
        , mMinDocCount(minDocCount)
        , mContinuousLen(continuousLen)
        , mDocInfo(nullptr)
        , mReference(nullptr)
    {
        mCounts.reserve(1024);
    }

public:
    const std::string& GetAttrName() { return mAttrName; }

public:
    bool Init(FieldType fieldType, const config::FieldConfigPtr& fieldConfig) override
    {
        auto refer = mDocInfoAllocator.DeclareReference(mAttrName, fieldType);
        mReference = static_cast<ReferenceTyped<T>*>(refer);

        auto evaluator
            = util::ClassTypedFactory<SortValueEvaluator, SortValueEvaluatorTyped>::GetInstance()
                  ->Create(fieldType);
        if (unlikely(!evaluator))
        {
            IE_LOG(ERROR, "create evaluator failed");
            return false;
        }
        evaluator->Init(fieldConfig, mReference);
        mEvaluator.reset(evaluator);

        mDocInfo = mDocInfoAllocator.Allocate();

        return true;
    }

public:
    void Process(const document::DocumentPtr& document) override
    {
        document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, document);
        const document::AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
        assert(attrDoc);

        mEvaluator->Evaluate(attrDoc, mDocInfo);
        T value {};
        mReference->Get(mDocInfo, value);
        DoProcess(value);
    }

    void ProcessForMerge(const AttributeSegmentReaderPtr& segReader, docid_t localId) override
    {
        T value = T();
        if (!segReader->Read(localId, reinterpret_cast<uint8_t*>(&value), sizeof(value)))
        {
            INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
        }
        DoProcess(value);
    }

    autil::legacy::json::JsonMap Dump() const override
    {
        autil::legacy::json::JsonMap ret;
        if (mCounts.empty())
        {
            return ret;
        }
        std::map<T, uint32_t> ordered(mCounts.begin(), mCounts.end());
        T lastKey {};
        uint32_t lastCount = 0;
        auto attrKey = TimeSeriesSegmentMetricsUpdater::GetAttrKey(mAttrName);
        for (auto iter = ordered.rbegin(); iter != ordered.rend(); iter++)
        {
            if (iter->second >= mMinDocCount)
            {
                if (lastCount != 0 && lastKey == iter->first + 1)
                {
                    ret[attrKey] = lastKey * mPeriod;
                    break;
                }
                else
                {
                    lastKey = iter->first;
                    lastCount = iter->second;
                }
            }
            else
            {
                lastCount = 0;
            }
        }
        if (ret.find(attrKey) == ret.end())
        {
            auto iter = ordered.rbegin();
            ret[attrKey] = iter->first * mPeriod;
            IE_LOG(WARN, "can not find valid baseline, use max value");
        }
        return ret;
    }

private:
    void DoProcess(T key)
    {
        T subKey = GetSubKey(key);
        auto iter = mCounts.find(subKey);
        if (mCounts.end() == iter)
        {
            mCounts[subKey] = 1;
        }
        else
        {
            iter->second = iter->second + 1;
        }
    }

    T GetSubKey(T key);

private:
    std::string mAttrName;
    size_t mPeriod;
    size_t mMinDocCount;
    size_t mContinuousLen;
    index::DocInfo* mDocInfo;
    ReferenceTyped<T>* mReference;
    std::unique_ptr<SortValueEvaluator> mEvaluator;
    index::DocInfoAllocator mDocInfoAllocator;
    std::unordered_map<T, uint32_t> mCounts;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIME_SERIES_SEGMENT_METRICS_UPDATER_H
