#ifndef __INDEXLIB_MULIT_VALUE_ATTRIBUTE_SEGMENT_READER_H
#define __INDEXLIB_MULIT_VALUE_ATTRIBUTE_SEGMENT_READER_H

#include <tr1/memory>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_defrag_slice_array.h"
#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"
#include "indexlib/index/normal/attribute/format/updatable_var_num_attribute_offset_formatter.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class MultiValueAttributeSegmentReader : public AttributeSegmentReader
{
public:
    MultiValueAttributeSegmentReader(
            const config::AttributeConfigPtr& config,
            AttributeMetrics* attributeMetrics = NULL);

    ~MultiValueAttributeSegmentReader() {}

public:
    bool IsInMemory() const override {return false;}
    uint32_t GetDataLength(docid_t docId) const override;
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override;
    uint64_t GetOffset(docid_t docId) const override;
    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override; 
    
public:
    void Open(const index_base::SegmentData& segData,
              const file_system::DirectoryPtr& attrDirectory = file_system::DirectoryPtr(),
              const AttributeSegmentPatchIteratorPtr& segmentPatchIterator = 
              AttributeSegmentPatchIteratorPtr());

    inline bool Read(docid_t docId, autil::MultiValueType<T>& value,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;
    
    inline bool Read(docid_t docId, autil::CountedMultiValueType<T>& value,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    uint32_t GetMaxDataItemLen() const { return mDataInfo.maxItemLen; }
    
private:
    inline const uint8_t* ReadData(
            docid_t docId, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

private:
    void InitSliceFileReader(const file_system::DirectoryPtr& attrDirectory);

    file_system::FileReaderPtr CreateFileReader(
            const file_system::DirectoryPtr& directory, 
            const std::string& fileName);

protected:
    uint32_t mDocCount;
    int32_t mFixedValueCount;
    AttributeOffsetReader mOffsetReader;
    UpdatableVarNumAttributeOffsetFormatter mOffsetFormatter;
    uint8_t* mData;
    VarNumAttributeDefragSliceArrayPtr mDefragSliceArray;
    config::CompressTypeOption mCompressType;

    VarNumAttributeDataFormatter mDataFormatter;
    file_system::FileReaderPtr mDataFile;
    file_system::SliceFileReaderPtr mSliceFile;
    config::AttributeConfigPtr mAttrConfig;
    AttributeDataInfo mDataInfo;

    static const uint32_t RESERVE_SLICE_NUM = 8 * 1024;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, MultiValueAttributeSegmentReader);
//////////////////////////////////////////////////////
template <typename T>
MultiValueAttributeSegmentReader<T>::MultiValueAttributeSegmentReader(
        const config::AttributeConfigPtr& config,
        AttributeMetrics* attributeMetrics)
    : AttributeSegmentReader(attributeMetrics)
    , mDocCount(0)
    , mFixedValueCount(-1)
    , mOffsetReader(config, attributeMetrics)
    , mData(NULL)
    , mAttrConfig(config)
{
    assert(config);
    mFixedValueCount = mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    mCompressType = mAttrConfig->GetCompressType();
}

template <typename T>
inline void MultiValueAttributeSegmentReader<T>::Open(
        const index_base::SegmentData& segData,
        const file_system::DirectoryPtr& attrDirectory,
        const AttributeSegmentPatchIteratorPtr& segmentPatchIterator)
{
    std::string attrName = mAttrConfig->GetAttrName();
    IE_LOG(DEBUG, "Start loading segment(%d) for attribute(%s)... ", 
           (int32_t)segData.GetSegmentId(), attrName.c_str());

    file_system::DirectoryPtr baseDirectory = attrDirectory;
    if (!baseDirectory) 
    {
        baseDirectory = segData.GetAttributeDirectory(attrName, true);
    }

    mDocCount = segData.GetSegmentInfo().docCount;
    mDataFormatter.Init(mAttrConfig);
    mOffsetReader.Init(baseDirectory, mDocCount);

    mDataFile = CreateFileReader(baseDirectory, ATTRIBUTE_DATA_FILE_NAME);
    mData = (uint8_t* )mDataFile->GetBaseAddress();
    mOffsetFormatter.Init(mDataFile->GetLength());

    if (mAttrConfig->IsAttributeUpdatable())
    {
        InitSliceFileReader(baseDirectory);
        if (segmentPatchIterator)
        {
            LoadPatches(segmentPatchIterator);
        }
    }
    IE_LOG(DEBUG, "Finishing loading segment(%d) for attribute(%s)... ", 
           (int32_t)segData.GetSegmentId(), attrName.c_str());

    // open data info file
    std::string dataInfoStr;
    baseDirectory->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfoStr);
    mDataInfo.FromString(dataInfoStr);
}

template <typename T>
void MultiValueAttributeSegmentReader<T>::InitSliceFileReader(
        const file_system::DirectoryPtr& attrDirectory)
{
    file_system::SliceFilePtr sliceFile;
    if (attrDirectory->IsExist(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME))
    {
        sliceFile = attrDirectory->GetSliceFile(
                ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME);
    }
    else
    {
        sliceFile = attrDirectory->CreateSliceFile(
                ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME,
                common::VAR_NUM_ATTRIBUTE_SLICE_LEN, RESERVE_SLICE_NUM);
    }
    mSliceFile = sliceFile->CreateSliceFileReader();
    const util::BytesAlignedSliceArrayPtr& sliceArray = 
        mSliceFile->GetBytesAlignedSliceArray();
    mDefragSliceArray.reset(new VarNumAttributeDefragSliceArray(
                    sliceArray, mAttrConfig->GetAttrName(),
                    mAttrConfig->GetDefragSlicePercent()));
    mDefragSliceArray->Init(&mOffsetReader, mOffsetFormatter,
                            mDataFormatter, mAttributeMetrics);
}

template <typename T>
inline file_system::FileReaderPtr 
MultiValueAttributeSegmentReader<T>::CreateFileReader(
        const file_system::DirectoryPtr& directory, const std::string& fileName)
{
    if (mAttrConfig->GetConfigType() == config::AttributeConfig::ct_section)  
    {
        return directory->CreateIntegratedFileReader(fileName);
    }
    return directory->CreateFileReader(fileName, file_system::FSOT_MMAP);
}

template <typename T>
inline const uint8_t* MultiValueAttributeSegmentReader<T>::ReadData(
        docid_t docId, autil::mem_pool::Pool* pool) const
{
    assert(docId >= 0 && docId < (docid_t)mDocCount);

    uint64_t offset = mOffsetReader.GetOffset(docId);
    const uint8_t* data = NULL;
    if (!mOffsetFormatter.IsSliceArrayOffset(offset))
    {
        data = mData + offset;
    }
    else
    {
        assert(mDefragSliceArray);
        data = (const uint8_t*)mDefragSliceArray->Get(
                mOffsetFormatter.DecodeToSliceArrayOffset(offset));
    }
    return data;
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::Read(docid_t docId,
        autil::MultiValueType<T>& value, autil::mem_pool::Pool* pool) const 
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }

    const uint8_t* data = ReadData(docId, pool);
    if (mFixedValueCount == -1)
    {
        value.init((const void*)data);
        return true;
    }
    if (!pool)
    {
        IE_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }
    
    autil::CountedMultiValueType<T> cValue((const void*)data, mFixedValueCount);
    size_t encodeCountLen = autil::MultiValueFormatter::getEncodedCountLength(cValue.size());
    size_t bufferLen = encodeCountLen + cValue.length();
    char* copyBuffer = (char*)pool->allocate(bufferLen);
    if (!copyBuffer)
    {
        return false;
    }
    autil::MultiValueFormatter::encodeCount(cValue.size(), copyBuffer, bufferLen);
    memcpy(copyBuffer + encodeCountLen, cValue.getBaseAddress(), bufferLen - encodeCountLen); 
    autil::MultiValueType<T> retValue(copyBuffer);
    value = retValue;
    return true;
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::Read(
        docid_t docId, autil::CountedMultiValueType<T>& value,
        autil::mem_pool::Pool* pool) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }
    const uint8_t* data = ReadData(docId, pool);
    if (mFixedValueCount == -1)
    {
        autil::MultiValueType<T> tValue;
        tValue.init((const void*)data);
        value = autil::CountedMultiValueType<T>(tValue);
        return true;
    }
    value = autil::CountedMultiValueType<T>((const void*)data, mFixedValueCount);
    return true;
}

template <>
inline bool MultiValueAttributeSegmentReader<float>::Read(docid_t docId,
        autil::MultiValueType<float>& value, autil::mem_pool::Pool* pool) const 
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }

    const uint8_t* data = ReadData(docId, pool);
    if (mFixedValueCount == -1)
    {
        value.init((const void*)data);
        return true;
    }
    
    if (!pool)
    {
        IE_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    return convertor.GetValue((const char*)data, value, pool);
}

template <>
inline bool MultiValueAttributeSegmentReader<float>::Read(
        docid_t docId, autil::CountedMultiValueType<float>& value,
        autil::mem_pool::Pool* pool) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }
    const uint8_t* data = ReadData(docId, pool);
    if (mFixedValueCount == -1)
    {
        autil::MultiValueType<float> tValue;
        tValue.init((const void*)data);
        value = autil::CountedMultiValueType<float>(tValue);
        return true;
    }
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    return convertor.GetValue((const char*)data, value, pool);
}

template <typename T>
inline uint64_t MultiValueAttributeSegmentReader<T>::GetOffset(docid_t docId) const
{
    return mOffsetReader.GetOffset(docId);
}

template <typename T>
inline uint32_t MultiValueAttributeSegmentReader<T>::GetDataLength(
        docid_t docId) const
{
    const uint8_t* data = ReadData(docId, NULL);
    return mDataFormatter.GetDataLength(data);
}

template <typename T> 
inline bool MultiValueAttributeSegmentReader<T>::UpdateField(
        docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    if (!mDefragSliceArray) // not updatable
    {
        return false;
    }
    
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }

    const uint8_t* data = ReadData(docId, NULL);
    uint32_t size = mDataFormatter.GetDataLength(data);

    if (size == bufLen && memcmp(data, buf, bufLen) == 0)
    {
        return true;
    }

    uint64_t offset = mDefragSliceArray->Append(buf, bufLen);
    uint64_t originalOffset = mOffsetReader.GetOffset(docId);
    mOffsetReader.SetOffset(
            docId, mOffsetFormatter.EncodeSliceArrayOffset(offset));

    if (mOffsetFormatter.IsSliceArrayOffset(originalOffset))
    {
        mDefragSliceArray->Free(
                mOffsetFormatter.DecodeToSliceArrayOffset(originalOffset), size);
    }
    return true;
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::Read(
        docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }

    const uint8_t* data = ReadData(docId, NULL);
    uint64_t dataLen = mDataFormatter.GetDataLength(data);
    if (dataLen > bufLen)
    {
        return false;
    }
    memcpy(buf, data, dataLen);
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULIT_VALUE_ATTRIBUTE_SEGMENT_READER_H
