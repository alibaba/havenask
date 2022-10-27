#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/dfs_var_num_attribute_offset_reverter.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeOffsetReader);

AttributeOffsetReader::AttributeOffsetReader(
        const AttributeConfigPtr& attrConfig,
        AttributeMetrics* attributeMetric)
    : mIsCompressOffset(false)
    , mAttrConfig(attrConfig)
    , mExpandSliceLen(0)
    , mAttributeMetrics(attributeMetric)
{
}

AttributeOffsetReader::~AttributeOffsetReader() 
{
}

void AttributeOffsetReader::Init(
        const DirectoryPtr& attrDirectory, uint32_t docCount)
{
    uint8_t* buffer = NULL;
    uint64_t bufferLen = 0;
    SliceFileReaderPtr sliceFile;

    // TODO: remove mIsCompressOffset when adaptive load compress or uncompress offset
    if (AttributeCompressInfo::NeedCompressOffset(mAttrConfig))
    {
        InitCompressOffsetData(attrDirectory, buffer, bufferLen, sliceFile);
    }
    else
    {
        InitUncompressOffsetData(attrDirectory, docCount, 
                buffer, bufferLen, sliceFile);
    }
    Init(buffer, bufferLen, docCount, sliceFile);
}

void AttributeOffsetReader::Init(
        uint8_t* buffer, uint64_t bufferLen, uint32_t docCount,
        SliceFileReaderPtr sliceFile)
{
    if (AttributeCompressInfo::NeedCompressOffset(mAttrConfig))
    {
        mIsCompressOffset = true;
        mCompressOffsetReader.Init(buffer, bufferLen, docCount, sliceFile);
    }
    else
    {
        mIsCompressOffset = false;
        mUncompressOffsetReader.Init(buffer, bufferLen, docCount, sliceFile);
    }

    mSliceFile = sliceFile;

    if (mIsCompressOffset && mAttrConfig->IsAttributeUpdatable() && mSliceFile)
    {
        mCompressMetrics = mCompressOffsetReader.GetUpdateMetrics();
        mExpandSliceLen = mSliceFile->GetLength();
        InitAttributeMetrics();

        IE_LOG(DEBUG, "EquivalentCompressUpdateMetrics %s offset, "
               "sliceFileBytes:%lu, noUseBytes:%lu, inplaceUpdate:%u, expandUpdate:%u",
               mAttrConfig->GetAttrName().c_str(),
               mSliceFile->GetLength(), mCompressMetrics.noUsedBytesSize, 
               mCompressMetrics.inplaceUpdateCount, mCompressMetrics.expandUpdateCount);
    }
}

void AttributeOffsetReader::InitCompressOffsetData(
        const DirectoryPtr& attrDirectory, 
        uint8_t*& baseAddress, uint64_t& offsetFileLen, 
        SliceFileReaderPtr& sliceFileReader)
{
    mOffsetFile = InitOffsetFile(attrDirectory);
    baseAddress = (uint8_t*)mOffsetFile->GetBaseAddress();
    offsetFileLen = mOffsetFile->GetLength();

    if (!mAttrConfig->IsAttributeUpdatable())
    {
        sliceFileReader = SliceFileReaderPtr();
        return;
    }

    string extendFileName = string(ATTRIBUTE_OFFSET_FILE_NAME)
                            + EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;
    SliceFilePtr sliceFile;
    if (attrDirectory->IsExist(extendFileName))
    {
        sliceFile = attrDirectory->GetSliceFile(extendFileName);
    }
    else
    {
        sliceFile = attrDirectory->CreateSliceFile(
                extendFileName, EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT);
    }
    sliceFileReader = sliceFile->CreateSliceFileReader();
}

void AttributeOffsetReader::InitUncompressOffsetData(
        const DirectoryPtr& attrDirectory, uint32_t docCount, 
        uint8_t*& baseAddress, uint64_t& offsetFileLen, 
        SliceFileReaderPtr& sliceFileReader)
{
    if (!mAttrConfig->IsAttributeUpdatable())
    {
        mOffsetFile = InitOffsetFile(attrDirectory);
        sliceFileReader = SliceFileReaderPtr();
        baseAddress = (uint8_t*)mOffsetFile->GetBaseAddress();
        offsetFileLen = mOffsetFile->GetLength();
        return;
    }

    size_t u64OffsetFileLen = (docCount + 1) * sizeof(uint64_t);
    string extendFileName = string(ATTRIBUTE_OFFSET_FILE_NAME) 
                            + ATTRIBUTE_OFFSET_EXTEND_SUFFIX;
    
    SliceFilePtr sliceFile;
    if (attrDirectory->IsExist(extendFileName))
    {
        sliceFile = attrDirectory->GetSliceFile(extendFileName);
    }
    else
    {
        sliceFile = attrDirectory->CreateSliceFile(
                extendFileName, u64OffsetFileLen, 1);
    }

    SliceFileReaderPtr fileReader = sliceFile->CreateSliceFileReader();
    size_t fileLen = fileReader->GetLength();
    if (fileLen != 0 && fileLen != u64OffsetFileLen)
    {
        INDEXLIB_FATAL_ERROR(
                IndexCollapsed,
                "Attribute offset slice file length [%lu]"
                " is not right, only can be 0 or %lu",
                fileLen, u64OffsetFileLen);
    }

    if (fileLen == u64OffsetFileLen)
    {
        // already extend to u64
        mOffsetFile = fileReader;
        sliceFileReader = SliceFileReaderPtr();
    }
    else
    {
        mOffsetFile = InitOffsetFile(attrDirectory);
        sliceFileReader = fileReader;
    }

    baseAddress = (uint8_t*)mOffsetFile->GetBaseAddress();
    offsetFileLen = mOffsetFile->GetLength();
}

FileReaderPtr AttributeOffsetReader::InitOffsetFile(const DirectoryPtr& attrDirectory)
{
    if (mAttrConfig->GetConfigType() == config::AttributeConfig::ct_section)  
    {
        return attrDirectory->CreateIntegratedFileReader(
                ATTRIBUTE_OFFSET_FILE_NAME);
    }

    return attrDirectory->CreateWritableFileReader(
            ATTRIBUTE_OFFSET_FILE_NAME, FSOT_MMAP);
}

void AttributeOffsetReader::InitAttributeMetrics()
{
    if (!mAttributeMetrics)
    {
        return;
    }
    mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(
            mExpandSliceLen);
    mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(
            mCompressMetrics.noUsedBytesSize);
}

IE_NAMESPACE_END(index);

