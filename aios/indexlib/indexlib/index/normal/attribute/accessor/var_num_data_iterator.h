#ifndef __INDEXLIB_VAR_NUM_DATA_ITERATOR_H
#define __INDEXLIB_VAR_NUM_DATA_ITERATOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_value_type_traits.h"
#include "indexlib/index/normal/attribute/accessor/dfs_var_num_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class VarNumDataIterator : public AttributeDataIterator
{
public:
    VarNumDataIterator(const config::AttributeConfigPtr& attrConfig)
        : AttributeDataIterator(attrConfig)
        , mIsDfsReader(true)
        , mDataLen(0)
        , mFixedValueCount(-1)
    {
        assert(attrConfig);
        mFixedValueCount = mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount();
        mCompressType = mAttrConfig->GetCompressType();
    }

    ~VarNumDataIterator() {}
    
public:
    bool Init(const index_base::PartitionDataPtr& partData,
              segmentid_t segId) override;
    void MoveToNext() override;
    std::string GetValueStr() const override;
    autil::ConstString GetValueBinaryStr(autil::mem_pool::Pool *pool) const override;
    
    autil::MultiValueType<T> GetValue(autil::mem_pool::Pool* pool = NULL) const;

    autil::ConstString GetRawIndexImageValue() const override;

private:
    void InitSegmentReader(const index_base::SegmentData& segData);
    void InitPatchReader(const index_base::PartitionDataPtr& partData,
                         segmentid_t segmentId);
    void EnsureDataBufferSize(uint32_t size);

    void ReadDocData(docid_t docId);
    
private:
    typedef index::DFSVarNumAttributeSegmentReader<T> DFSSegReader;
    typedef index::MultiValueAttributeSegmentReader<T> IntegrateSegReader;
    typedef index::VarNumAttributePatchReader<T> PatchReader;
    DEFINE_SHARED_PTR(DFSSegReader);
    DEFINE_SHARED_PTR(IntegrateSegReader);
    DEFINE_SHARED_PTR(PatchReader);
    static const size_t MAX_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;
    
private:
    bool mIsDfsReader;
    index::AttributeSegmentReaderPtr mSegReader;
    PatchReaderPtr mPatchReader;
    std::vector<uint8_t> mDataBuf;
    uint32_t mDataLen;
    int32_t mFixedValueCount;
    config::CompressTypeOption mCompressType;
    mutable autil::mem_pool::Pool mPool;
    
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumDataIterator);

/////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool VarNumDataIterator<T>::Init(
        const index_base::PartitionDataPtr& partData, segmentid_t segId)
{
    index_base::SegmentData segData = partData->GetSegmentData(segId);
    mDocCount = segData.GetSegmentInfo().docCount;
    if (mDocCount <= 0)
    {
        IE_LOG(INFO, "segment [%d] is empty, will not open segment reader.", segId);
        return true;
    }

    InitSegmentReader(segData);
    InitPatchReader(partData, segId);
    mCurDocId = 0;
    ReadDocData(mCurDocId);
    return true;
}

template <typename T>
inline void VarNumDataIterator<T>::MoveToNext()
{
    ++mCurDocId;
    if (mCurDocId >= mDocCount)
    {
        IE_LOG(INFO, "reach eof!");
        return;
    }
    ReadDocData(mCurDocId);
}

template <typename T>
inline autil::ConstString VarNumDataIterator<T>::GetRawIndexImageValue() const
{
    return autil::ConstString((const char*)mDataBuf.data(), mDataLen);
}

template <typename T>
inline autil::MultiValueType<T> VarNumDataIterator<T>::GetValue(
        autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<T> value;
    if (!pool)
    {
        value.init((const void*)mDataBuf.data());
    }
    else
    {
        char* copyBuf = (char*)pool->allocate(mDataLen);
        memcpy(copyBuf, mDataBuf.data(), mDataLen);
        value.init((const void*)copyBuf);
    }
    return value;
}

template <>
inline autil::MultiValueType<float> VarNumDataIterator<float>::GetValue(
        autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<float> value;
    if (!pool)
    {
        if (mFixedValueCount == -1)
        {
            value.init((const void*)mDataBuf.data());
            return value;
        }
        INDEXLIB_FATAL_ERROR(
                UnSupported,
                "pool is nullptr when read MultiValueType value from fixed length attribute [%s]",
                mAttrConfig->GetAttrName().c_str());
    }

    if (mFixedValueCount == -1)
    {
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
    if (mPool.getUsedBytes() >= MAX_POOL_MEMORY_THRESHOLD)
    {
        mPool.release();
    }
    autil::MultiValueType<T> value = GetValue(&mPool);
    return index::AttributeValueTypeToString<autil::MultiValueType<T>>::ToString(value);
}

template <typename T>
inline autil::ConstString VarNumDataIterator<T>::GetValueBinaryStr(
        autil::mem_pool::Pool *pool) const
{
    auto value = GetValue(pool);
    return autil::ConstString(value.getBaseAddress(), value.length());
}

template <typename T>
inline void VarNumDataIterator<T>::InitSegmentReader(
        const index_base::SegmentData& segmentData)
{
    mIsDfsReader = !mAttrConfig->IsUniqEncode();
    if (mIsDfsReader)
    {
        IE_LOG(INFO, "create dfs segment reader for attribute [%s]",
               mAttrConfig->GetAttrName().c_str());
        DFSSegReaderPtr segReader(new DFSSegReader(mAttrConfig));
        file_system::DirectoryPtr attrDir = segmentData.GetAttributeDirectory(
                mAttrConfig->GetAttrName(), true);
        segReader->Open(attrDir, segmentData.GetSegmentInfo());
        EnsureDataBufferSize(segReader->GetMaxDataItemLen());
        mSegReader = segReader;
    }
    else
    {
        IE_LOG(INFO, "create integrate segment reader for attribute [%s]",
               mAttrConfig->GetAttrName().c_str());
        IntegrateSegReaderPtr segReader(new IntegrateSegReader(mAttrConfig));
        segReader->Open(segmentData);
        EnsureDataBufferSize(segReader->GetMaxDataItemLen());
        mSegReader = segReader;
    }
}

template <typename T>
inline void VarNumDataIterator<T>::InitPatchReader(
        const index_base::PartitionDataPtr& partData, segmentid_t segmentId)
{
    if (!mAttrConfig->IsAttributeUpdatable())
    {
        return;
    }

    index_base::PatchFileFinderPtr patchFinder = 
        index_base::PatchFileFinderCreator::Create(partData.get());
    index_base::AttrPatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForSegment(mAttrConfig, segmentId, patchVec);

    IE_LOG(INFO, "Init PatchReader with [%lu] patch files", patchVec.size());
    mPatchReader.reset(new PatchReader(mAttrConfig));
    for (size_t i = 0; i < patchVec.size(); i++)
    {
        mPatchReader->AddPatchFile(patchVec[i].patchDirectory,
                patchVec[i].patchFileName, patchVec[i].srcSegment);
    }
    EnsureDataBufferSize(mPatchReader->GetMaxPatchItemLen());
}

template <typename T>
inline void VarNumDataIterator<T>::EnsureDataBufferSize(uint32_t size)
{
    if (mDataBuf.size() >= (size_t)size)
    {
        return;
    }
    mDataBuf.resize(size);
}

template <typename T>
inline void VarNumDataIterator<T>::ReadDocData(docid_t docId)
{
    if (!mSegReader)
    {
        IE_LOG(ERROR, "segment reader is NULL!");
        return;
    }

    uint8_t* valueBuf = (uint8_t*)mDataBuf.data();
    if (mPatchReader)
    {
        mDataLen = mPatchReader->Seek(docId, valueBuf, mDataBuf.size());
        if (mDataLen > 0)
        {
            // read from patch
            return;
        }
    }
    
    if (mIsDfsReader)
    {
        DFSSegReader* segReader = static_cast<DFSSegReader*>(mSegReader.get());
        if (!segReader->ReadDataAndLen(docId, valueBuf, mDataBuf.size(), mDataLen))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "read value for doc [%d] failed!", docId);
        }
    }
    else
    {
        IntegrateSegReader* segReader = static_cast<IntegrateSegReader*>(mSegReader.get());
        if (!segReader->Read(docId, valueBuf, mDataBuf.size()))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "read value for doc [%d] failed!", docId);
        }
        mDataLen = segReader->GetDataLength(docId);
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_DATA_ITERATOR_H
