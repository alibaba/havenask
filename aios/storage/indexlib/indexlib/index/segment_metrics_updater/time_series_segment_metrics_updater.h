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

#include <memory>
#include <unordered_map>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/sort_value_evaluator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/class_typed_factory.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib { namespace index {

class TimeSeriesSegmentMetricsProcessor
{
public:
    TimeSeriesSegmentMetricsProcessor() {};
    virtual ~TimeSeriesSegmentMetricsProcessor() {};

public:
    virtual const std::string& GetAttrName() = 0;

public:
    virtual bool Init(FieldType fieldType, const config::AttributeConfigPtr& fieldConfig) = 0;
    virtual void Process(const document::DocumentPtr& doc) = 0;
    virtual void ProcessForMerge(const AttributeSegmentReaderWithCtx& segReader, docid_t localId) = 0;
    virtual autil::legacy::json::JsonMap Dump() const = 0;
};

class TimeSeriesSegmentMetricsUpdater : public index::SegmentMetricsUpdater
{
public:
    TimeSeriesSegmentMetricsUpdater(util::MetricProviderPtr metricProvider);
    ~TimeSeriesSegmentMetricsUpdater();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              const util::KeyValueMap& parameters) override;
    bool InitForMerge(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const util::KeyValueMap& parameters) override;
    std::string GetUpdaterName() const override { return TimeSeriesSegmentMetricsUpdater::UPDATER_NAME; }

public:
    void Update(const document::DocumentPtr& doc) override;
    void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue) override;
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
    static std::string GetAttrKey(const std::string& attrName) { return BASELINE_IDENTIFIER + ":" + attrName; }

    template <typename T>
    static bool GetBaseline(std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics,
                            const std::string& attrName, T& baseline)
    {
        return segmentGroupMetrics->Get(GetAttrKey(attrName), baseline);
    }

private:
    autil::mem_pool::Pool mPool;
    TimeSeriesSegmentMetricsProcessor* mProcessor;
    index::OfflineAttributeSegmentReaderContainerPtr mReaderContainer;
    std::unordered_map<segmentid_t, AttributeSegmentReaderWithCtx> mSegReaderMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimeSeriesSegmentMetricsUpdater);

template <typename T>
class TimeSeriesSegmentMetricsProcessorImpl : public TimeSeriesSegmentMetricsProcessor
{
public:
    TimeSeriesSegmentMetricsProcessorImpl(std::string& attrName, size_t period, size_t minDocCount,
                                          size_t continuousLen)
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
    const std::string& GetAttrName() override { return mAttrName; }

public:
    bool Init(FieldType fieldType, const config::AttributeConfigPtr& attrConfig) override
    {
        auto fieldConfig = attrConfig->GetFieldConfig();
        auto refer = mDocInfoAllocator.DeclareReference(mAttrName, fieldType, fieldConfig->IsEnableNullField());
        mReference = static_cast<index::legacy::ReferenceTyped<T>*>(refer);

        auto evaluator =
            util::ClassTypedFactory<SortValueEvaluator, SortValueEvaluatorTyped>::GetInstance()->Create(fieldType);
        if (unlikely(!evaluator)) {
            IE_LOG(ERROR, "create evaluator failed");
            return false;
        }
        evaluator->Init(attrConfig, mReference);
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
        bool isNull = false;
        mReference->Get(mDocInfo, value, isNull);
        if (!isNull) {
            DoProcess(value);
        }
    }

    void ProcessForMerge(const AttributeSegmentReaderWithCtx& segReader, docid_t localId) override
    {
        T value = T();
        bool isNull = false;
        if (!segReader.reader->Read(localId, segReader.ctx, reinterpret_cast<uint8_t*>(&value), sizeof(value),
                                    isNull)) {
            INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
        }
        if (!isNull) {
            DoProcess(value);
        }
    }

    autil::legacy::json::JsonMap Dump() const override
    {
        autil::legacy::json::JsonMap ret;
        if (mCounts.empty()) {
            return ret;
        }
        std::map<T, uint32_t> ordered(mCounts.begin(), mCounts.end());
        T lastKey {};
        uint32_t lastCount = 0;
        auto attrKey = TimeSeriesSegmentMetricsUpdater::GetAttrKey(mAttrName);
        for (auto iter = ordered.rbegin(); iter != ordered.rend(); iter++) {
            if (iter->second >= mMinDocCount) {
                if (lastCount != 0 && lastKey == iter->first + 1) {
                    ret[attrKey] = lastKey * mPeriod;
                    break;
                } else {
                    lastKey = iter->first;
                    lastCount = iter->second;
                }
            } else {
                lastCount = 0;
            }
        }
        if (ret.find(attrKey) == ret.end()) {
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
        if (mCounts.end() == iter) {
            mCounts[subKey] = 1;
        } else {
            iter->second = iter->second + 1;
        }
    }

    T GetSubKey(T key);

private:
    std::string mAttrName;
    size_t mPeriod;
    size_t mMinDocCount;
    size_t mContinuousLen;
    legacy::DocInfo* mDocInfo;
    legacy::ReferenceTyped<T>* mReference;
    std::unique_ptr<SortValueEvaluator> mEvaluator;
    legacy::DocInfoAllocator mDocInfoAllocator;
    std::unordered_map<T, uint32_t> mCounts;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
