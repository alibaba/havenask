#ifndef __INDEXLIB_ATTRIBUTE_OFFSET_READER_H
#define __INDEXLIB_ATTRIBUTE_OFFSET_READER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/uncompress_offset_reader.h"
#include "indexlib/index/normal/attribute/accessor/compress_offset_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/slice_file.h"

IE_NAMESPACE_BEGIN(index);

class AttributeOffsetReader
{
public:
    AttributeOffsetReader(const config::AttributeConfigPtr& attrConfig,
                          AttributeMetrics* attributeMetric = NULL);
    ~AttributeOffsetReader();

public:
    void Init(const file_system::DirectoryPtr& attrDirectory, uint32_t docCount);

    // public for dfs_segment_reader
    void Init(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount,
              file_system::SliceFileReaderPtr sliceFile = 
              file_system::SliceFileReaderPtr());

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;
    bool SetOffset(docid_t docId, uint64_t offset);
    inline bool IsU32Offset() const __ALWAYS_INLINE;

    uint32_t GetDocCount() const 
    { 
        if (mIsCompressOffset)
        {
            return mCompressOffsetReader.GetDocCount();
        }
        return mUncompressOffsetReader.GetDocCount();
    }

private:
    void InitCompressOffsetData(
            const file_system::DirectoryPtr& attrDirectory, 
            uint8_t*& baseAddress, uint64_t& offsetFileLen, 
            file_system::SliceFileReaderPtr& sliceFileReader);

    void InitUncompressOffsetData(
            const file_system::DirectoryPtr& attrDirectory, 
            uint32_t docCount, uint8_t*& baseAddress, uint64_t& offsetFileLen, 
            file_system::SliceFileReaderPtr& sliceFileReader);

    file_system::FileReaderPtr InitOffsetFile(
            const file_system::DirectoryPtr& attrDirectory);

    void InitAttributeMetrics();

    void UpdateMetrics();

private:
    bool mIsCompressOffset;
    CompressOffsetReader mCompressOffsetReader;
    UncompressOffsetReader mUncompressOffsetReader;

    file_system::FileReaderPtr mOffsetFile;
    file_system::SliceFileReaderPtr mSliceFile; 
    config::AttributeConfigPtr mAttrConfig;

    common::EquivalentCompressUpdateMetrics mCompressMetrics;
    size_t mExpandSliceLen;
    AttributeMetrics* mAttributeMetrics;

private:
    friend class AttributeOffsetReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeOffsetReader);
///////////////////////////////////////////////////

inline uint64_t AttributeOffsetReader::GetOffset(docid_t docId) const
{
    if (mIsCompressOffset)
    {
        return mCompressOffsetReader.GetOffset(docId);
    }
    return mUncompressOffsetReader.GetOffset(docId);
}

inline bool AttributeOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (mIsCompressOffset)
    {
        bool ret = mCompressOffsetReader.SetOffset(docId, offset);
        UpdateMetrics();
        return ret;
    }

    if (offset > mAttrConfig->GetFieldConfig()->GetU32OffsetThreshold()
        && mUncompressOffsetReader.IsU32Offset())
    {
        mUncompressOffsetReader.ExtendU32OffsetToU64Offset(mSliceFile);
        assert(!mUncompressOffsetReader.IsU32Offset());
    }
    return mUncompressOffsetReader.SetOffset(docId, offset);
}

inline void AttributeOffsetReader::UpdateMetrics()
{
    if (!mIsCompressOffset || !mAttrConfig->IsAttributeUpdatable() || !mSliceFile)
    {
        return;
    }

    common::EquivalentCompressUpdateMetrics newMetrics = 
        mCompressOffsetReader.GetUpdateMetrics();
    size_t newSliceFileLen = mSliceFile->GetLength();
    if (mAttributeMetrics)
    {
        mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(
                newSliceFileLen - mExpandSliceLen);
        mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(
                newMetrics.noUsedBytesSize - mCompressMetrics.noUsedBytesSize);
        mAttributeMetrics->IncreaseEqualCompressInplaceUpdateCountValue(
                newMetrics.inplaceUpdateCount - mCompressMetrics.inplaceUpdateCount);
        mAttributeMetrics->IncreaseEqualCompressExpandUpdateCountValue(
                newMetrics.expandUpdateCount - mCompressMetrics.expandUpdateCount);
    }
    mExpandSliceLen = newSliceFileLen;
    mCompressMetrics = newMetrics;
}

inline bool AttributeOffsetReader::IsU32Offset() const
{
    if (mIsCompressOffset)
    {
        return mCompressOffsetReader.IsU32Offset();
    }
    return mUncompressOffsetReader.IsU32Offset();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_OFFSET_READER_H
