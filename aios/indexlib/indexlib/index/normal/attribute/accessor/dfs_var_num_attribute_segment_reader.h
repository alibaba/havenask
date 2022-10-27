#ifndef __INDEXLIB_DFS_VAR_NUM_ATTRIBUTE_SEGMENT_READER_H
#define __INDEXLIB_DFS_VAR_NUM_ATTRIBUTE_SEGMENT_READER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <autil/MultiValueType.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/config/pack_attribute_config.h"

IE_NAMESPACE_BEGIN(index);

// for merge only!!!
template <typename T>
class DFSVarNumAttributeSegmentReader : public AttributeSegmentReader
{
public:
    DFSVarNumAttributeSegmentReader(config::AttributeConfigPtr attrConfig);
    ~DFSVarNumAttributeSegmentReader();
public:
    bool IsInMemory() const override { return false; }
    uint32_t GetDataLength(docid_t docId) const override;
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override;
    uint64_t GetOffset(docid_t docId) const override
    { return mOffsetReader->GetOffset(docId); }

    bool ReadDataAndLen(docid_t docId, uint8_t* buf, 
            uint32_t bufLen, uint32_t &dataLen) override;
    void Open(const file_system::DirectoryPtr& attrDirectory, 
              const index_base::SegmentInfo& segInfo);

public:
    static int64_t EstimateMemoryUse(uint32_t docCount);
    bool ReadCount(uint8_t* buffPtr, uint64_t offset, 
                   size_t &encodeCountLen, uint32_t &count);

    uint32_t GetMaxDataItemLen() const { return mDataInfo.maxItemLen; }

private:
    bool DoReadLenInData(docid_t docId, uint8_t* buf,
                         uint32_t bufLen, uint32_t &dataLen);
    
protected:
    file_system::FileReaderPtr mOffsetFile;
    file_system::BufferedFileReaderPtr mDataFile;
    AttributeOffsetReaderPtr mOffsetReader;
    uint8_t* mOffset;
    uint32_t mDocCount;
    AttributeDataInfo mDataInfo;
    int32_t mFixedValueCount;
    uint32_t mFixedValueLength;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, DFSVarNumAttributeSegmentReader);

template <typename T>
DFSVarNumAttributeSegmentReader<T>::DFSVarNumAttributeSegmentReader(
        config::AttributeConfigPtr attrConfig)
    : mOffset(NULL)
    , mDocCount(0)
    , mFixedValueCount(-1)
    , mFixedValueLength(0)
{
    mOffsetReader.reset(new AttributeOffsetReader(attrConfig));
    mFixedValueCount = attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    if (mFixedValueCount != -1)
    {
        mFixedValueLength = config::PackAttributeConfig::GetFixLenFieldSize(
                attrConfig->GetFieldConfig());
    }
}

template <typename T>
DFSVarNumAttributeSegmentReader<T>::~DFSVarNumAttributeSegmentReader()
{
    mOffset = NULL;
}

template <typename T>
void DFSVarNumAttributeSegmentReader<T>::Open(
        const file_system::DirectoryPtr& attrDirectory,
        const index_base::SegmentInfo& segInfo)
{
    // Load offset file.
    mOffsetFile = attrDirectory->CreateFileReader(
            ATTRIBUTE_OFFSET_FILE_NAME, file_system::FSOT_IN_MEM);
    mOffset = (uint8_t*)mOffsetFile->GetBaseAddress();

    mOffsetReader->Init(mOffset, mOffsetFile->GetLength(), segInfo.docCount);
    mDocCount = segInfo.docCount;

    // open data file.
    file_system::FileReaderPtr fileReader = 
        attrDirectory->CreateFileReader(ATTRIBUTE_DATA_FILE_NAME,
                file_system::FSOT_BUFFERED);
    mDataFile = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, fileReader);
    assert(mDataFile);

    // open data info file
    std::string dataInfoStr;
    attrDirectory->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfoStr);
    mDataInfo.FromString(dataInfoStr);
}

template <typename T>
uint32_t DFSVarNumAttributeSegmentReader<T>::GetDataLength(docid_t docId) const
{
    assert(false); // useless
    return uint32_t(-1);
}

template <typename T>
bool DFSVarNumAttributeSegmentReader<T>::Read(
        docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    assert(false); // useless
    return false;
}

template <typename T>
bool DFSVarNumAttributeSegmentReader<T>::ReadDataAndLen(
        docid_t docId, uint8_t* buf,
        uint32_t bufLen, uint32_t &dataLen)
{
    return DoReadLenInData(docId, buf, bufLen, dataLen);
}

template <typename T>
inline bool DFSVarNumAttributeSegmentReader<T>::DoReadLenInData(
        docid_t docId, uint8_t* buf,
        uint32_t bufLen, uint32_t &dataLen)
{
    uint64_t offset = GetOffset(docId);
    size_t encodeCountLen = 0;
    uint32_t itemCount = 0;
    if (!ReadCount(buf, offset, encodeCountLen, itemCount))
    {
        return false;
    }
    uint32_t itemLen = (mFixedValueCount != -1) ? mFixedValueLength : (itemCount * sizeof(T));
    dataLen = itemLen + encodeCountLen;
    assert(dataLen <= bufLen);
    size_t retLen = mDataFile->Read(buf + encodeCountLen,
                                    itemLen, offset + encodeCountLen);
    return (retLen == itemLen);
}

template <>
inline bool DFSVarNumAttributeSegmentReader<autil::MultiChar>::DoReadLenInData(
        docid_t docId, uint8_t* buf,
        uint32_t bufLen, uint32_t &dataLen)
{
    uint8_t* begin = buf;
    uint64_t offset = GetOffset(docId);
    size_t encodeCountLen = 0;
    uint32_t itemCount = 0;
    if (!ReadCount(buf, offset, encodeCountLen, itemCount))
    {
        return false;
    }
    if (itemCount == 0)
    {
        dataLen = encodeCountLen;
        return true;
    }
    buf += encodeCountLen;
    offset += encodeCountLen;

    // read offset len
    size_t retLen = mDataFile->Read(buf, sizeof(uint8_t), offset);
    if (retLen != sizeof(uint8_t))
    {
        return false;
    }
    uint8_t offsetLen = *buf;
    ++buf;
    ++offset;

    // read offsets
    uint32_t offsetVecLen = itemCount * offsetLen;
    retLen = mDataFile->Read(buf, offsetVecLen, offset);
    if (retLen != offsetVecLen)
    {
        return false;
    }
    uint32_t lastOffset = common::VarNumAttributeFormatter::GetOffset(
            (const char*)buf, offsetLen, itemCount - 1);
    buf += offsetVecLen;
    offset += offsetVecLen;

    // read data besides last item
    retLen = mDataFile->Read(buf, lastOffset, offset);
    if (retLen != lastOffset)
    {
        return false;
    }
    buf += lastOffset;
    offset += lastOffset;

    // read last item count
    size_t lastItemCountLen = 0;
    uint32_t lastItemCount = 0;
    if (!ReadCount(buf, offset, lastItemCountLen, lastItemCount))
    {
        return false;
    }
    buf += lastItemCountLen;
    offset += lastItemCountLen;

    retLen = mDataFile->Read(buf, lastItemCount, offset);
    if (retLen != lastItemCount)
    {
        return false;
    }
    buf += lastItemCount;
    offset += lastItemCount;
    dataLen = buf - begin;
    return true;
}

template <typename T>
inline int64_t DFSVarNumAttributeSegmentReader<T>::EstimateMemoryUse(
        uint32_t docCount)
{
    int64_t size = sizeof(DFSVarNumAttributeSegmentReader<T>);
    size += file_system::FSReaderParam::DEFAULT_BUFFER_SIZE; // data buffer
    size += docCount * sizeof(int64_t); // offset buffer, worst situation
    return size;
}

template <typename T>
inline bool DFSVarNumAttributeSegmentReader<T>::ReadCount(
        uint8_t* buffPtr, uint64_t offset, 
        size_t &encodeCountLen, uint32_t &count)
{
    if (mFixedValueCount != -1)
    {
        count = (uint32_t)mFixedValueCount;
        encodeCountLen = 0;
        return true;
    }
    
    size_t retLen = mDataFile->Read(buffPtr, sizeof(uint8_t), offset);
    if (retLen != sizeof(uint8_t))
    {
        return false;
    }

    encodeCountLen = 
        common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*buffPtr);
    // read remain bytes for count
    retLen = mDataFile->Read(buffPtr + sizeof(uint8_t), encodeCountLen - 1, 
                             offset + sizeof(uint8_t));
    if (retLen != encodeCountLen - 1)
    {
        return false;
    }
    count = common::VarNumAttributeFormatter::DecodeCount(
            (const char*)buffPtr, encodeCountLen);
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DFS_VAR_NUM_ATTRIBUTE_SEGMENT_READER_H
