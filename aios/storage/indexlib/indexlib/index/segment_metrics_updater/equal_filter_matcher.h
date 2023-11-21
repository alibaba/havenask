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

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encode_var_num_attribute_segment_reader_for_offline.h"
#include "indexlib/index/segment_metrics_updater/buildin_match_function_factory.h"
#include "indexlib/index/segment_metrics_updater/filter_matcher.h"
#include "indexlib/index/segment_metrics_updater/sort_value_evaluator.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlib { namespace index {

template <typename AttrType, typename RangeType>
class EqualFilterMatcher : public FilterMatcher
{
public:
    EqualFilterMatcher() : mMaxLen(0), mIsNull(false) {}
    ~EqualFilterMatcher()
    {
        mCacheCtx.clear();
        mBuff.Release();
    }

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
    index::AttributeSegmentReader::ReadContextBasePtr GetReadCtx(const std::string& key,
                                                                 const index::AttributeSegmentReaderPtr& reader);
    void ValueEqual(const AttrType& currentValue, std::vector<int32_t>& matchIds);

private:
    SortValueEvaluatorTyped<AttrType> mEvaluator;
    config::AttributeConfigPtr mAttrConfig;
    index::OfflineAttributeSegmentReaderContainerPtr mAttributeReaders;
    std::map<std::string, index::AttributeSegmentReaderPtr> mCacheReader;
    std::map<std::string, index::AttributeSegmentReader::ReadContextBasePtr> mCacheCtx;
    autil::mem_pool::Pool mCtxPool;
    std::string mFieldName;
    std::map<int32_t, RangeType> mExpectValues;
    RangeType mCurrentValue;
    AttrType mCurrent;
    uint64_t mMaxLen;
    bool mIsNull;
    util::MemBuffer mBuff;

private:
    IE_LOG_DECLARE();
};

template <typename AttrType, typename RangeType>
bool EqualFilterMatcher<AttrType, RangeType>::Init(const config::AttributeSchemaPtr& attrSchema,
                                                   const config::ConditionFilterPtr& conditionFilter, int32_t idx)
{
    auto attrConfig = attrSchema->GetAttributeConfig(conditionFilter->GetFieldName());
    assert(attrConfig);
    mAttrConfig = attrConfig;
    mEvaluator.InitForTemplate(attrConfig);
    if (!InitFunction(conditionFilter)) {
        return false;
    }
    return AddValue(conditionFilter->GetValue(), idx);
}

template <typename AttrType, typename RangeType>
bool EqualFilterMatcher<AttrType, RangeType>::InitFunction(const config::ConditionFilterPtr& conditionFilter)
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
bool EqualFilterMatcher<AttrType, RangeType>::InitForMerge(
    const config::AttributeSchemaPtr& attrSchema, const config::ConditionFilterPtr& conditionFilter,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders, int32_t idx)
{
    auto attrConfig = attrSchema->GetAttributeConfig(conditionFilter->GetFieldName());
    assert(attrConfig);
    mAttrConfig = attrConfig;
    mEvaluator.InitForTemplate(attrConfig);
    mAttributeReaders = attrReaders;
    mFieldName = conditionFilter->GetFieldName();
    if (!InitFunction(conditionFilter)) {
        return false;
    }
    return AddValue(conditionFilter->GetValue(), idx);
}

template <typename AttrType, typename RangeType>
bool EqualFilterMatcher<AttrType, RangeType>::AddValue(const std::string& value, int32_t idx)
{
    RangeType v;
    if (!autil::StringUtil::fromString(value, v)) {
        IE_LOG(ERROR, "equal value [%s] is invalid", value.c_str());
        return false;
    }
    mExpectValues.insert(std::make_pair(idx, v));
    return true;
}

template <typename AttrType, typename RangeType>
index::AttributeSegmentReader::ReadContextBasePtr
EqualFilterMatcher<AttrType, RangeType>::GetReadCtx(const std::string& key,
                                                    const index::AttributeSegmentReaderPtr& reader)
{
    if (mCtxPool.getUsedBytes() >= autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE) {
        mCacheCtx.clear();
        mCtxPool.release();
    }
    auto iter = mCacheCtx.find(key);
    if (iter == mCacheCtx.end()) {
        auto ctx = reader->CreateReadContextPtr(&mCtxPool);
        mCacheCtx.insert(make_pair(key, ctx));
        return ctx;
    }
    return iter->second;
}

template <typename AttrType, typename RangeType>
void EqualFilterMatcher<AttrType, RangeType>::Match(const document::DocumentPtr& doc, std::vector<int32_t>& matchIds)
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
    return ValueEqual(mCurrentValue, matchIds);
}

template <typename AttrType, typename RangeType>
void EqualFilterMatcher<AttrType, RangeType>::ValueEqual(const AttrType& currentValue, std::vector<int32_t>& matchIds)
{
    for (auto iter = mExpectValues.begin(); iter != mExpectValues.end(); iter++) {
        if (typeid(AttrType) == typeid(double) || typeid(RangeType) == typeid(double)) {
            if (std::fabs((double)(mCurrentValue - iter->second)) < FilterMatcher::EPS) {
                matchIds.push_back(iter->first);
            }
        } else if (mCurrentValue == iter->second) {
            matchIds.push_back(iter->first);
        }
    }
}

template <>
inline void EqualFilterMatcher<std::string, std::string>::ValueEqual(const std::string& currentValue,
                                                                     std::vector<int32_t>& matchIds)
{
    for (auto iter = mExpectValues.begin(); iter != mExpectValues.end(); iter++) {
        if (mCurrentValue == iter->second) {
            matchIds.push_back(iter->first);
        }
    }
}

template <>
inline void EqualFilterMatcher<std::string, std::string>::Match(const document::DocumentPtr& doc,
                                                                std::vector<int32_t>& matchIds)
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
    return ValueEqual(mCurrentValue, matchIds);
}

template <typename AttrType, typename RangeType>
void EqualFilterMatcher<AttrType, RangeType>::Match(segmentid_t segId, docid_t localId, std::vector<int32_t>& matchIds)
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
    return ValueEqual(mCurrentValue, matchIds);
}

template <>
inline void EqualFilterMatcher<std::string, std::string>::Match(segmentid_t segId, docid_t localId,
                                                                std::vector<int32_t>& matchIds)
{
    std::string key = autil::StringUtil::toString(segId) + "_" + mFieldName;
    AttributeSegmentReaderPtr reader;
    auto iter = mCacheReader.find(key);
    if (iter == mCacheReader.end()) {
        reader = mAttributeReaders->GetAttributeSegmentReader(mFieldName, segId);
        uint32_t strLen =
            mAttrConfig->IsUniqEncode()
                ? std::static_pointer_cast<UniqEncodeVarNumAttributeSegmentReaderForOffline<char>>(reader)
                      ->GetMaxDataItemLen()
                : std::static_pointer_cast<MultiValueAttributeSegmentReader<char>>(reader)->GetMaxDataItemLen();
        if (strLen > mMaxLen) {
            mMaxLen = strLen;
            mBuff.Release();
            mBuff.Reserve(mMaxLen);
        }
        mCacheReader.insert(std::make_pair(key, reader));
    } else {
        reader = iter->second;
    }
    assert(reader);
    uint32_t dataLen;
    if (!reader->ReadDataAndLen(localId, GetReadCtx(key, reader), (uint8_t*)mBuff.GetBuffer(), mBuff.GetBufferSize(),
                                dataLen)) {
        INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
    }

    autil::MultiChar multiChar(mBuff.GetBuffer());
    mIsNull = multiChar.isNull();
    if (mIsNull) {
        return;
    }
    mCurrentValue = std::string(multiChar.data(), multiChar.size());
    if (mFunction) {
        mFunction->Execute((int8_t*)&mCurrent, (int8_t*)&mCurrentValue);
    }
    return ValueEqual(mCurrentValue, matchIds);
}

IE_LOG_SETUP_TEMPLATE2(index, EqualFilterMatcher);
}} // namespace indexlib::index
