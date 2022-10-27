#ifndef __INDEXLIB_COMPRESS_FILE_READER_H
#define __INDEXLIB_COMPRESS_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/compress_file_info.h"
#include "indexlib/file_system/compress_file_address_mapper.h"
#include "indexlib/util/buffer_compressor/buffer_compressor_creator.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(file_system);

class CompressFileReader : public FileReader
{
public:
    CompressFileReader(autil::mem_pool::Pool* pool = NULL) 
        : mCompressor(NULL)
        , mOffset(0) 
        , mCurBlockIdx(0)
        , mPool(pool)
    {}

    virtual ~CompressFileReader()
    {
        POOL_COMPATIBLE_DELETE_CLASS(mPool, mCompressor);
    }

public:
    virtual bool Init(const FileReaderPtr& fileReader,
                      const CompressFileInfo& compressInfo, Directory* directory);
    
    virtual CompressFileReader* CreateSessionReader(autil::mem_pool::Pool* pool) = 0;
    
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
    }
    FSOpenType GetOpenType() override
    {
        if (mDataFileReader)
        {
            return mDataFileReader->GetOpenType();
        }
        return FSOT_UNKNOWN;
    }

public:

    size_t GetUncompressedFileLength() const { return mCompressInfo.deCompressFileLen; }
    FileReaderPtr GetDataFileReader() const { return mDataFileReader; }
    const CompressFileInfo& GetCompressInfo() const { return mCompressInfo; }
    
protected:
    bool DoInit(const FileReaderPtr& fileReader, 
                const CompressFileAddressMapperPtr& compressAddressMapper,
                const CompressFileInfo& compressInfo);

    CompressFileAddressMapperPtr LoadCompressAddressMapper(
            const FileReaderPtr& fileReader,
            const CompressFileInfo& compressInfo, Directory* directory);

    util::BufferCompressor* CreateCompressor(
            const CompressFileAddressMapperPtr& compressAddressMapper,
            const CompressFileInfo& compressInfo, autil::mem_pool::Pool* pool);

    bool InCurrentBlock(size_t offset) const
    {
        assert(mCompressor);
        assert(offset < GetUncompressedFileLength());
        return mCompressAddrMapper->OffsetToBlockIdx(offset) == mCurBlockIdx;
    }

protected:
    virtual void LoadBuffer(size_t offset, ReadOption option) = 0;
    
    FileNodePtr GetFileNode() const override
    {
        assert(false);
        return FileNodePtr();
    }

private:
    void PrefetchData(size_t length, size_t offset, ReadOption option);

protected:
    CompressFileAddressMapperPtr mCompressAddrMapper; 
    util::BufferCompressor* mCompressor;
    size_t mOffset;
    size_t mCurBlockIdx;
    CompressFileInfo mCompressInfo;
    FileReaderPtr mDataFileReader;
    autil::mem_pool::Pool* mPool;
        
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFileReader);

class CompressFileReaderGuard
{
public:
    CompressFileReaderGuard(CompressFileReader* reader,
                            autil::mem_pool::Pool* pool)
        : mReader(reader)
        , mPool(pool)
    {
        assert(mReader);
    }

    ~CompressFileReaderGuard()
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mReader);
    }
    
private:
    CompressFileReader* mReader;
    autil::mem_pool::Pool* mPool;
};

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESS_FILE_READER_H
