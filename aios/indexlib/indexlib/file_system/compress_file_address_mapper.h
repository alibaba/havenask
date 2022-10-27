#ifndef __INDEXLIB_COMPRESS_FILE_ADDRESS_MAPPER_H
#define __INDEXLIB_COMPRESS_FILE_ADDRESS_MAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/compress_file_info.h"
#include "indexlib/file_system/slice_file.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(file_system);

class CompressFileAddressMapper
{
public:
    CompressFileAddressMapper();
    ~CompressFileAddressMapper();
    
public:
    void InitForRead(const CompressFileInfo& fileInfo,
                     const file_system::FileReaderPtr& reader,
                     Directory* directory);

    void InitForWrite(size_t blockSize);

    void Dump(const file_system::FileWriterPtr& writer);
    
public:
    void AddOneBlock(size_t compBlockSize);

    size_t GetBlockSize() const { return (size_t)(1 << mPowerOf2); }
        
    size_t GetBlockCount() const
    { return mBlockCount; }
    
    char* Data() const
    { return (char*)mBaseAddr; }
    
    size_t Size() const
    { return (mBlockCount + 1) * sizeof(size_t); }

    size_t GetCompressFileLength() const
    {
        assert(mBaseAddr);
        return mBaseAddr[mBlockCount];
    }

    size_t OffsetToBlockIdx(int64_t offset) const { return offset >> mPowerOf2; }
    size_t OffsetToInBlockOffset(int64_t offset) const { return offset & mPowerMark; }
    size_t CompressBlockAddress(int64_t blockIdx) const
    {
        assert((size_t)blockIdx < mBlockCount);
        return mBaseAddr[blockIdx];
    }
    size_t CompressBlockLength(int64_t blockIdx) const
    {
        assert((size_t)blockIdx < mBlockCount);
        return mBaseAddr[blockIdx + 1] - mBaseAddr[blockIdx];
    }

    size_t GetMaxCompressBlockSize();
    
private:
    void InitBlockMask(size_t blockSize);
    
    std::string GetBlockOffsetMapperFileName(
            const FileReaderPtr& reader, const Directory* directory);
    
private:
    size_t* mBaseAddr;
    uint32_t mPowerOf2;
    uint32_t mPowerMark;
    size_t mBlockCount;
    std::vector<size_t> mBlockOffsetVec;
    size_t mMaxCompressBlockSize;
    SliceFilePtr mBlockOffsetSliceFile;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFileAddressMapper);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESS_FILE_ADDRESS_MAPPER_H
