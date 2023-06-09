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
#ifndef __INDEXLIB_VAR_NUM_DATA_ITERATOR_H
#define __INDEXLIB_VAR_NUM_DATA_ITERATOR_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encode_var_num_attribute_segment_reader_for_offline.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumDataIterator : public AttributeDataIterator
{
private:
    typedef index::MultiValueAttributeSegmentReader<T> DFSSegReader;
    typedef index::UniqEncodeVarNumAttributeSegmentReaderForOffline<T> IntegrateSegReader;
    typedef index::VarNumAttributePatchReader<T> PatchReader;
    DEFINE_SHARED_PTR(DFSSegReader);
    DEFINE_SHARED_PTR(IntegrateSegReader);
    DEFINE_SHARED_PTR(PatchReader);
    static const size_t MAX_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;

public:
    VarNumDataIterator(const config::AttributeConfigPtr& attrConfig)
        : AttributeDataIterator(attrConfig)
        , mIsNull(false)
        , mDataLen(0)
        , mFixedValueCount(-1)
    {
        assert(attrConfig);
        mFixedValueCount = mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount();
        mCompressType = mAttrConfig->GetCompressType();
    }

    ~VarNumDataIterator() { mReadCtx.reset(); }

public:
    bool Init(const index_base::PartitionDataPtr& partData, segmentid_t segId) override;
    void MoveToNext() override;
    std::string GetValueStr() const override;
    autil::StringView GetValueBinaryStr(autil::mem_pool::Pool* pool) const override;

    autil::MultiValueType<T> GetValue(autil::mem_pool::Pool* pool = NULL) const;

    autil::StringView GetRawIndexImageValue() const override;

    bool IsNullValue() const override { return mIsNull; }

private:
    void InitSegmentReader(const index_base::PartitionDataPtr& partData, const index_base::SegmentData& segData);
    PatchReaderPtr GetPatchReader(const index_base::PartitionDataPtr& partData, segmentid_t segmentId);
    void EnsureDataBufferSize(uint32_t size);

    void UpdateIsNull();
    void ReadDocData(docid_t docId);

private:
    bool mIsNull;
    index::AttributeSegmentReaderPtr mSegReader;
    mutable index::AttributeSegmentReader::ReadContextBasePtr mReadCtx;
    std::vector<uint8_t> mDataBuf;
    uint32_t mDataLen;
    int32_t mFixedValueCount;
    config::CompressTypeOption mCompressType;
    mutable autil::mem_pool::Pool mPool;
    mutable autil::mem_pool::Pool mCtxPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumDataIterator);

/////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool VarNumDataIterator<T>::Init(const index_base::PartitionDataPtr& partData, segmentid_t segId)
{
    index_base::SegmentData segData = partData->GetSegmentData(segId);
    mDocCount = segData.GetSegmentInfo()->docCount;
    if (mDocCount <= 0) {
        IE_LOG(INFO, "segment [%d] is empty, will not open segment reader.", segId);
        return true;
    }
    InitSegmentReader(partData, segData);
    mCurDocId = 0;
    ReadDocData(mCurDocId);
    return true;
}

template <typename T>
inline void VarNumDataIterator<T>::MoveToNext()
{
    ++mCurDocId;
    if (mCurDocId >= mDocCount) {
        IE_LOG(INFO, "reach eof!");
        return;
    }
    ReadDocData(mCurDocId);
}

template <typename T>
inline autil::StringView VarNumDataIterator<T>::GetRawIndexImageValue() const
{
    return autil::StringView((const char*)mDataBuf.data(), mDataLen);
}

template <typename T>
inline autil::MultiValueType<T> VarNumDataIterator<T>::GetValue(autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<T> value;
    if (!pool) {
        if (mFixedValueCount == -1) {
            value.init((const void*)mDataBuf.data());
            return value;
        }

        INDEXLIB_FATAL_ERROR(UnSupported,
                             "pool is nullptr when read MultiValueType value from fixed length attribute [%s]",
                             mAttrConfig->GetAttrName().c_str());
    }

    if (mFixedValueCount == -1) {
        char* copyBuf = (char*)pool->allocate(mDataLen);
        memcpy(copyBuf, mDataBuf.data(), mDataLen);
        value.init((const void*)copyBuf);
    } else {
        size_t len = common::VarNumAttributeFormatter::GetEncodedCountLength(mFixedValueCount) + mDataLen;
        char* copyBuf = (char*)pool->allocate(len);
        size_t encodeCountLen = common::VarNumAttributeFormatter::EncodeCount(mFixedValueCount, copyBuf, len);
        memcpy(copyBuf + encodeCountLen, mDataBuf.data(), mDataLen);
        value.init((const void*)copyBuf);
    }
    return value;
}

template <>
inline autil::MultiValueType<float> VarNumDataIterator<float>::GetValue(autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<float> value;
    if (!pool) {
        if (mFixedValueCount == -1) {
            value.init((const void*)mDataBuf.data());
            return value;
        }
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "pool is nullptr when read MultiValueType value from fixed length attribute [%s]",
                             mAttrConfig->GetAttrName().c_str());
    }

    if (mFixedValueCount == -1) {
        char* copyBuf = (char*)pool->allocate(mDataLen);
        memcpy(copyBuf, mDataBuf.data(), mDataLen);
        value.init((const void*)copyBuf);
        return value;
    }
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    convertor.GetValue((const char*)mDataBuf.data(), value, pool);
    return value;
}

template <typename T>
inline std::string VarNumDataIterator<T>::GetValueStr() const
{
    if (mPool.getUsedBytes() >= MAX_POOL_MEMORY_THRESHOLD) {
        mPool.release();
    }
    autil::MultiValueType<T> value = GetValue(&mPool);
    if (value.isNull()) {
        return mAttrConfig->GetFieldConfig()->GetNullFieldLiteralString();
    }
    return index::AttributeValueTypeToString<autil::MultiValueType<T>>::ToString(value);
}

template <typename T>
inline autil::StringView VarNumDataIterator<T>::GetValueBinaryStr(autil::mem_pool::Pool* pool) const
{
    auto value = GetValue(pool);
    if (mFixedValueCount == -1) {
        return autil::StringView(value.getBaseAddress(), value.length());
    } else {
        return autil::StringView(value.getData(), value.getDataSize());
    }
}

template <typename T>
inline void VarNumDataIterator<T>::InitSegmentReader(const index_base::PartitionDataPtr& partData,
                                                     const index_base::SegmentData& segmentData)
{
    bool isDfsReader = !mAttrConfig->IsUniqEncode();
    if (isDfsReader) {
        IE_LOG(INFO, "create dfs segment reader for attribute [%s]", mAttrConfig->GetAttrName().c_str());
        auto patchReader = GetPatchReader(partData, segmentData.GetSegmentId());
        DFSSegReaderPtr segReader(new DFSSegReader(mAttrConfig));
        file_system::DirectoryPtr attrDir = segmentData.GetAttributeDirectory(mAttrConfig->GetAttrName(), true);
        segReader->Open(segmentData, PatchApplyOption::OnRead(patchReader), attrDir, nullptr, true);
        assert(segReader->GetBaseAddress() == nullptr);
        EnsureDataBufferSize(segReader->GetMaxDataItemLen());
        mSegReader = segReader;
    } else {
        IE_LOG(INFO, "create integrate segment reader for attribute [%s]", mAttrConfig->GetAttrName().c_str());
        IntegrateSegReaderPtr segReader(new IntegrateSegReader(mAttrConfig));
        segReader->Open(partData, *segmentData.GetSegmentInfo(), segmentData.GetSegmentId());
        EnsureDataBufferSize(segReader->GetMaxDataItemLen());
        mSegReader = segReader;
    }
}

template <typename T>
inline typename VarNumDataIterator<T>::PatchReaderPtr
VarNumDataIterator<T>::GetPatchReader(const index_base::PartitionDataPtr& partData, segmentid_t segmentId)
{
    if (!mAttrConfig->IsAttributeUpdatable()) {
        return nullptr;
    }

    index_base::PatchFileFinderPtr patchFinder = index_base::PatchFileFinderCreator::Create(partData.get());
    index_base::PatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForTargetSegment(mAttrConfig, segmentId, &patchVec);

    IE_LOG(INFO, "Init PatchReader with [%lu] patch files", patchVec.size());
    PatchReaderPtr patchReader(new PatchReader(mAttrConfig));
    for (size_t i = 0; i < patchVec.size(); i++) {
        patchReader->AddPatchFile(patchVec[i].patchDirectory, patchVec[i].patchFileName, patchVec[i].srcSegment);
    }
    EnsureDataBufferSize(patchReader->GetMaxPatchItemLen());
    return patchReader;
}

template <typename T>
inline void VarNumDataIterator<T>::EnsureDataBufferSize(uint32_t size)
{
    if (mDataBuf.size() >= (size_t)size) {
        return;
    }
    mDataBuf.resize(size);
}

template <typename T>
inline void VarNumDataIterator<T>::ReadDocData(docid_t docId)
{
    if (!mSegReader) {
        IE_LOG(ERROR, "segment reader is NULL!");
        return;
    }

    uint8_t* valueBuf = (uint8_t*)mDataBuf.data();
    if (mCtxPool.getUsedBytes() >= MAX_POOL_MEMORY_THRESHOLD || !mReadCtx) {
        mReadCtx.reset();
        mCtxPool.release();
        mReadCtx = mSegReader->CreateReadContextPtr(&mCtxPool);
    }
    if (!mSegReader->ReadDataAndLen(docId, mReadCtx, valueBuf, mDataBuf.size(), mDataLen)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "read value for doc [%d] failed!", docId);
    }
    UpdateIsNull();
}

template <typename T>
inline void VarNumDataIterator<T>::UpdateIsNull()
{
    if (mFixedValueCount != -1) {
        mIsNull = false;
        return;
    }
    size_t encodeCountLen = 0;
    common::VarNumAttributeFormatter::DecodeCount((const char*)mDataBuf.data(), encodeCountLen, mIsNull);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_VAR_NUM_DATA_ITERATOR_H
