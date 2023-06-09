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
#ifndef __INDEXLIB_SINGLE_VALUE_DATA_ITERATOR_H
#define __INDEXLIB_SINGLE_VALUE_DATA_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueDataIterator : public AttributeDataIterator
{
public:
    SingleValueDataIterator(const config::AttributeConfigPtr& attrConfig)
        : AttributeDataIterator(attrConfig)
        , mFieldType(attrConfig->GetFieldConfig()->GetFieldType())
        , mValue(T())
        , mIsNull(false)
    {
    }

    ~SingleValueDataIterator() { mReadCtx.reset(); }

public:
    bool Init(const index_base::PartitionDataPtr& partData, segmentid_t segId) override;
    void MoveToNext() override;
    std::string GetValueStr() const override;
    autil::StringView GetValueBinaryStr(autil::mem_pool::Pool* pool) const override;
    autil::StringView GetRawIndexImageValue() const override;
    bool IsNullValue() const override { return mIsNull; }
    T GetValue() const;

private:
    typedef index::SingleValueAttributeSegmentReader<T> SegmentReader;
    typedef std::shared_ptr<SegmentReader> SegmentReaderPtr;

    autil::mem_pool::Pool mPool;
    SegmentReaderPtr mSegReader;
    AttributeSegmentReader::ReadContextBasePtr mReadCtx;
    FieldType mFieldType;
    T mValue;
    bool mIsNull;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueDataIterator);
//////////////////////////////////////////////////////////////////////
template <typename T>
inline bool SingleValueDataIterator<T>::Init(const index_base::PartitionDataPtr& partData, segmentid_t segId)
{
    index_base::SegmentData segData = partData->GetSegmentData(segId);
    mDocCount = segData.GetSegmentInfo()->docCount;
    if (mDocCount <= 0) {
        IE_LOG(INFO, "segment [%d] is empty, will not open segment reader.", segId);
        return true;
    }

    IE_LOG(INFO, "Init dfs segment reader for single attribute [%s]", mAttrConfig->GetAttrName().c_str());
    mSegReader.reset(new SegmentReader(mAttrConfig));
    PatchApplyOption patchOption;
    patchOption.applyStrategy = PAS_APPLY_ON_READ;
    AttributeSegmentPatchIteratorPtr patchIterator(AttributeSegmentPatchIteratorCreator::Create(mAttrConfig));
    patchIterator->Init(partData, segId);
    patchOption.patchReader = patchIterator;
    mSegReader->Open(segData, patchOption, nullptr, true);
    if (mSegReader->GetDataBaseAddr() != nullptr) {
        assert(false);
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "expected open in block cache, but get base address!");
        return false;
    }
    mReadCtx = mSegReader->CreateReadContextPtr(&mPool);

    mCurDocId = 0;
    if (!mSegReader->Read(mCurDocId, mReadCtx, (uint8_t*)&mValue, sizeof(T), mIsNull)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "read value for doc [%d] failed!", mCurDocId);
        return false;
    }
    return true;
}

template <typename T>
inline void SingleValueDataIterator<T>::MoveToNext()
{
    ++mCurDocId;
    if (mCurDocId >= mDocCount) {
        IE_LOG(INFO, "reach eof!");
        return;
    }
    if (!mSegReader->Read(mCurDocId, mReadCtx, (uint8_t*)&mValue, sizeof(T), mIsNull)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "read value for doc [%d] failed!", mCurDocId);
    }
}

template <typename T>
inline T SingleValueDataIterator<T>::GetValue() const
{
    return mValue;
}

template <typename T>
inline std::string SingleValueDataIterator<T>::GetValueStr() const
{
    if (mIsNull) {
        return mAttrConfig->GetFieldConfig()->GetNullFieldLiteralString();
    }

    T value = GetValue();
    return index::AttributeValueTypeToString<T>::ToString(value);
}

template <>
inline std::string SingleValueDataIterator<uint32_t>::GetValueStr() const
{
    if (mIsNull) {
        return mAttrConfig->GetFieldConfig()->GetNullFieldLiteralString();
    }

    uint32_t value = GetValue();
    if (mFieldType == ft_time) {
        util::TimestampUtil::TM tm = util::TimestampUtil::ConvertToTM(value);
        char buffer[128];
        snprintf(buffer, 128, "%02lu:%02lu:%02lu.%03lu", (uint64_t)tm.hour, (uint64_t)tm.minute, (uint64_t)tm.second,
                 (uint64_t)tm.millisecond);
        return std::string(buffer);
    }
    if (mFieldType == ft_date) {
        util::TimestampUtil::TM tm = util::TimestampUtil::ConvertToTM(value * util::TimestampUtil::DAY_MILLION_SEC);
        char buffer[128];
        snprintf(buffer, 128, "%04lu-%02lu-%02lu", tm.GetADYear(), (uint64_t)tm.month, (uint64_t)tm.day);
        return std::string(buffer);
    }
    return index::AttributeValueTypeToString<uint32_t>::ToString(value);
}

template <>
inline std::string SingleValueDataIterator<uint64_t>::GetValueStr() const
{
    if (mIsNull) {
        return mAttrConfig->GetFieldConfig()->GetNullFieldLiteralString();
    }

    uint64_t value = GetValue();
    if (mFieldType == ft_timestamp) {
        util::TimestampUtil::TM tm = util::TimestampUtil::ConvertToTM(value);
        char buffer[128];
        snprintf(buffer, 128, "%04lu-%02lu-%02lu %02lu:%02lu:%02lu.%03lu", tm.GetADYear(), (uint64_t)tm.month,
                 (uint64_t)tm.day, (uint64_t)tm.hour, (uint64_t)tm.minute, (uint64_t)tm.second,
                 (uint64_t)tm.millisecond);
        return std::string(buffer);
    }
    return index::AttributeValueTypeToString<uint64_t>::ToString(value);
}

template <typename T>
inline autil::StringView SingleValueDataIterator<T>::GetRawIndexImageValue() const
{
    return autil::StringView((const char*)&mValue, sizeof(T));
}

template <typename T>
inline autil::StringView SingleValueDataIterator<T>::GetValueBinaryStr(autil::mem_pool::Pool* pool) const
{
    char* copyBuf = (char*)pool->allocate(sizeof(T));
    *(T*)copyBuf = mValue;
    return autil::StringView((const char*)copyBuf, sizeof(T));
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_DATA_ITERATOR_H
