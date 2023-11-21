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

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/segment_metrics_updater/buildin_match_function_factory.h"
#include "indexlib/index/segment_metrics_updater/filter_matcher.h"
#include "indexlib/index/segment_metrics_updater/sort_value_evaluator.h"

namespace indexlib { namespace index {

template <typename AttrType, typename RangeType>
class RangeValue
{
public:
    RangeValue(RangeType leftValue, RangeType rightValue, bool strictLeft, bool strictRight)
        : mLeftValue(leftValue)
        , mRightValue(rightValue)
        , mStrictLeft(strictLeft)
        , mStrictRight(strictRight)
    {
    }

    bool MatchRange(AttrType currentValue)
    {
        if (typeid(AttrType) == typeid(double) || typeid(RangeType) == typeid(double)) {
            if ((std::fabs((double)(currentValue - mLeftValue)) < FilterMatcher::EPS) && mStrictLeft) {
                return true;
            }
            if ((std::fabs((double)(currentValue - mRightValue)) < FilterMatcher::EPS) && mStrictRight) {
                return true;
            }
        }
        if (currentValue < mLeftValue || (currentValue == mLeftValue && !mStrictLeft) ||
            (currentValue > mRightValue || (currentValue == mRightValue && !mStrictRight))) {
            return false;
        }
        return true;
    }

private:
    RangeType mLeftValue;
    RangeType mRightValue;
    bool mStrictLeft;
    bool mStrictRight;
};

template <typename AttrType, typename RangeType>
class RangeFilterMatcher : public FilterMatcher
{
public:
    RangeFilterMatcher() : mCurrent(AttrType()), mIsNull(false) {}
    ~RangeFilterMatcher() { mCacheCtx.clear(); }

public:
    bool Init(const config::AttributeSchemaPtr& attrSchema, const config::ConditionFilterPtr& conditionFilter,
              int32_t idx) override;
    bool InitForMerge(const config::AttributeSchemaPtr& attrSchema, const config::ConditionFilterPtr& conditionFilter,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders, int32_t idx) override;
    void Match(const document::DocumentPtr& doc, std::vector<int32_t>& matchIds) override;
    void Match(segmentid_t segId, docid_t localId, std::vector<int32_t>& matchIds) override;
    bool AddValue(const std::string& value, int32_t idx) override;

private:
    bool InitFunction(const config::ConditionFilterPtr& conditionFilter);
    bool InitRangeValue(const std::string& rangeValue);
    void MatchRange(std::vector<int32_t>& matchIds);
    index::AttributeSegmentReader::ReadContextBasePtr GetReadCtx(const std::string& key,
                                                                 const index::AttributeSegmentReaderPtr& reader);
    bool AnalyseValue(const std::string& valueStr, RangeType& value);

private:
    SortValueEvaluatorTyped<AttrType> mEvaluator;
    index::OfflineAttributeSegmentReaderContainerPtr mAttributeReaders;
    std::map<std::string, index::AttributeSegmentReaderPtr> mCacheReader;
    std::map<std::string, index::AttributeSegmentReader::ReadContextBasePtr> mCacheCtx;
    autil::mem_pool::Pool mCtxPool;
    std::string mFieldName;
    RangeType mCurrentValue;
    std::map<int32_t, RangeValue<AttrType, RangeType>> mExpectRanges;
    AttrType mCurrent;
    bool mIsNull;

private:
    IE_LOG_DECLARE();
};
/////////////////////

IE_LOG_SETUP_TEMPLATE2(index, RangeFilterMatcher);

template <typename AttrType, typename RangeType>
bool RangeFilterMatcher<AttrType, RangeType>::Init(const config::AttributeSchemaPtr& attrSchema,
                                                   const config::ConditionFilterPtr& conditionFilter, int32_t idx)
{
    mFieldName = conditionFilter->GetFieldName();
    auto attrConfig = attrSchema->GetAttributeConfig(mFieldName);
    assert(attrConfig);
    mEvaluator.InitForTemplate(attrConfig);
    if (!InitFunction(conditionFilter)) {
        return false;
    }
    return AddValue(conditionFilter->GetValue(), idx);
}

template <typename AttrType, typename RangeType>
bool RangeFilterMatcher<AttrType, RangeType>::InitFunction(const config::ConditionFilterPtr& conditionFilter)
{
    std::string funcName = conditionFilter->GetFunctionName();
    if (!funcName.empty()) {
        mFunction = BuildinMatchFunctionFactory::CreateFunction<AttrType, RangeType>(
            funcName, conditionFilter->GetFunctionParam());
        if (mFunction == nullptr) {
            IE_LOG(ERROR, "create function [%s] failed", funcName.c_str());
            return false;
        }
    }
    return true;
}

template <typename AttrType, typename RangeType>
bool RangeFilterMatcher<AttrType, RangeType>::InitForMerge(
    const config::AttributeSchemaPtr& attrSchema, const config::ConditionFilterPtr& conditionFilter,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders, int32_t idx)
{
    auto attrConfig = attrSchema->GetAttributeConfig(conditionFilter->GetFieldName());
    assert(attrConfig);
    mEvaluator.InitForTemplate(attrConfig);
    mAttributeReaders = attrReaders;
    mFieldName = conditionFilter->GetFieldName();
    if (!InitFunction(conditionFilter)) {
        return false;
    }
    return AddValue(conditionFilter->GetValue(), idx);
}

template <typename AttrType, typename RangeType>
bool RangeFilterMatcher<AttrType, RangeType>::AddValue(const std::string& rangeValue, int32_t idx)
{
    bool strictRight = false, strictLeft = false;
    RangeType right = 0, left = 0;
    std::vector<std::string> values;
    autil::StringUtil::split(values, rangeValue, ",");
    assert(values.size() == 2);
    autil::StringUtil::trim(values[0]);
    autil::StringUtil::trim(values[1]);
    if (values[0][0] == '[') {
        strictLeft = true;
    }
    if (values[0][0] != '[' && values[0][0] != '(') {
        IE_LOG(ERROR, "rangle value [%s] is invalid", rangeValue.c_str());
        return false;
    }
    values[0].erase(0, 1);
    autil::StringUtil::trim(values[0]);

    if (values[1][values[1].length() - 1] == ']') {
        strictRight = true;
    }
    if (values[1][values[1].length() - 1] != ']' && values[1][values[1].length() - 1] != ')') {
        IE_LOG(ERROR, "rangle value [%s] is invalid", rangeValue.c_str());
        return false;
    }
    values[1].erase(values[1].length() - 1, 1);
    autil::StringUtil::trim(values[1]);

    if (!AnalyseValue(values[0], left) || !AnalyseValue(values[1], right) || left > right) {
        IE_LOG(ERROR, "rangle value [%s] is invalid", rangeValue.c_str());
        return false;
    }

    RangeValue<AttrType, RangeType> value(left, right, strictLeft, strictRight);
    mExpectRanges.insert(std::make_pair(idx, value));
    return true;
}

template <typename AttrType, typename RangeType>
bool RangeFilterMatcher<AttrType, RangeType>::AnalyseValue(const std::string& valueStr, RangeType& value)
{
    return autil::StringUtil::fromString<RangeType>(valueStr, value);
}

template <typename AttrType, typename RangeType>
index::AttributeSegmentReader::ReadContextBasePtr
RangeFilterMatcher<AttrType, RangeType>::GetReadCtx(const std::string& key,
                                                    const index::AttributeSegmentReaderPtr& reader)
{
    if (mCtxPool.getUsedBytes() >= autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE) {
        mCacheCtx.clear();
        mCtxPool.release();
    }
    auto iter = mCacheCtx.find(key);
    if (iter == mCacheCtx.end()) {
        auto ctx = reader->CreateReadContextPtr(&mCtxPool);
        mCacheCtx.insert(std::make_pair(key, ctx));
        return ctx;
    }
    return iter->second;
}

template <typename AttrType, typename RangeType>
void RangeFilterMatcher<AttrType, RangeType>::Match(const document::DocumentPtr& doc, std::vector<int32_t>& matchIds)
{
    document::NormalDocumentPtr normalDoc = std::static_pointer_cast<document::NormalDocument>(doc);
    assert(normalDoc);
    const document::AttributeDocumentPtr attrDoc = normalDoc->GetAttributeDocument();
    assert(attrDoc);
    mIsNull = false;
    mCurrent = mEvaluator.Evaluate(attrDoc, &mIsNull);
    if (mIsNull) {
        return;
    }
    if (mFunction) {
        mFunction->Execute((int8_t*)&mCurrent, (int8_t*)&mCurrentValue);
    } else {
        mCurrentValue = mCurrent;
    }
    return MatchRange(matchIds);
}

template <typename AttrType, typename RangeType>
void RangeFilterMatcher<AttrType, RangeType>::Match(segmentid_t segId, docid_t localId, std::vector<int32_t>& matchIds)
{
    std::string key = autil::StringUtil::toString(segId) + "_" + mFieldName;
    AttributeSegmentReaderPtr reader;
    auto iter = mCacheReader.find(key);
    if (iter == mCacheReader.end()) {
        reader = mAttributeReaders->GetAttributeSegmentReader(mFieldName, segId);
        mCacheReader.insert(std::make_pair(key, reader));
    } else {
        reader = iter->second;
    }
    assert(reader);
    mIsNull = false;
    if (!reader->Read(localId, GetReadCtx(key, reader), reinterpret_cast<uint8_t*>(&mCurrent), sizeof(mCurrent),
                      mIsNull)) {
        INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
    }
    if (mIsNull) {
        return;
    }
    if (mFunction) {
        mFunction->Execute((int8_t*)&mCurrent, (int8_t*)&mCurrentValue);
    } else {
        mCurrentValue = mCurrent;
    }
    return MatchRange(matchIds);
}

template <typename AttrType, typename RangeType>
void RangeFilterMatcher<AttrType, RangeType>::MatchRange(std::vector<int32_t>& matchIds)
{
    for (auto iter = mExpectRanges.begin(); iter != mExpectRanges.end(); iter++) {
        if (iter->second.MatchRange(mCurrentValue)) {
            matchIds.push_back(iter->first);
        }
    }
}

template <>
inline void RangeFilterMatcher<std::string, std::string>::MatchRange(std::vector<int32_t>& matchIds)
{
    // unsurpport <string, string> in RangeFilterMatcher
    assert(false);
}
}} // namespace indexlib::index
