#ifndef __INDEXLIB_SNAPPY_COMPRESS_FILE_READER_H
#define __INDEXLIB_SNAPPY_COMPRESS_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/util/buffer_compressor/snappy_compressor.h"

DECLARE_REFERENCE_CLASS(file_system, CompressFileMeta);
IE_NAMESPACE_BEGIN(file_system);

class SnappyCompressFileReader : public FileReader
{
public:
    SnappyCompressFileReader()
        : mCurInBlockOffset(0)
        , mCurBlockBeginOffset(0)
        , mOffset(0) 
        , mOriFileLength(0)
        , mCurBlockIdx(0)
        , mBlockSize(0)
    {}

    ~SnappyCompressFileReader() {}

public:
    bool Init(const FileReaderPtr& fileReader);

public:
    void Open() override {}
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Read(void* buffer, size_t length, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, 
                             "CompressReader does not support Read ByteSliceList");
        return NULL;
    }
    void* GetBaseAddress() const override
    {
        return NULL;
    }
    size_t GetLength() const override
    {
        assert(mDataFileReader);
        return mDataFileReader->GetLength();
    }
    const std::string& GetPath() const override
    {
        assert(mDataFileReader);
        return mDataFileReader->GetPath();
    }
    void Close() override 
    {
        if (mDataFileReader)
        {
            mDataFileReader->Close();
            mDataFileReader.reset();
        }
    };
    FSOpenType GetOpenType() override
    {
        if (mDataFileReader)
        {
            mDataFileReader->GetOpenType();
        }
        return FSOT_UNKNOWN;
    }

    size_t GetUncompressedFileLength() const;

    FileReaderPtr GetDataFileReader() const { return mDataFileReader; }
    
private:
    bool Init(const FileReaderPtr& fileReader, 
              const CompressFileMetaPtr& compressMeta, size_t blockSize);

    CompressFileMetaPtr LoadCompressMeta(const FileReaderPtr& fileReader);
    bool InCurrentBlock(size_t offset) const;
    bool LocateBlockOffsetInCompressFile(
            size_t offset, size_t &blockIdx, size_t &uncompBlockOffset, 
            size_t &compBlockOffset, size_t &compBlockSize) const;

    void LoadBuffer(size_t offset, ReadOption option);

private:
    FileNodePtr GetFileNode() const override
    {
        assert(false);
        return FileNodePtr();
    }
private:
    FileReaderPtr mDataFileReader;
    CompressFileMetaPtr mCompressMeta; 
    util::SnappyCompressor mCompressor;
    size_t mCurInBlockOffset;
    size_t mCurBlockBeginOffset;
    size_t mOffset;
    size_t mOriFileLength;
    size_t mCurBlockIdx;
    size_t mBlockSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SnappyCompressFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SNAPPY_COMPRESS_FILE_READER_H
