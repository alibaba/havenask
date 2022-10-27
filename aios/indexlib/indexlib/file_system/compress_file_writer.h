#ifndef __INDEXLIB_COMPRESS_FILE_WRITER_H
#define __INDEXLIB_COMPRESS_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"

DECLARE_REFERENCE_CLASS(util, BufferCompressor);
DECLARE_REFERENCE_CLASS(file_system, CompressFileAddressMapper);

IE_NAMESPACE_BEGIN(file_system);

class CompressFileWriter : public FileWriter
{
public:
    CompressFileWriter();
    ~CompressFileWriter();

public:
    static const size_t DEFAULT_COMPRESS_BUFF_SIZE = 10 * 1024 * 1024; // 10MB

public:
    void Open(const std::string& path) override
    { assert(false); }

public:
    void Init(const FileWriterPtr& fileWriter,
              const FileWriterPtr& InfoFileWriter,
              const std::string& compressorName,
              size_t bufferSize);

    size_t Write(const void* buffer, size_t length) override;
    size_t GetLength() const override
    {
        if (mDataWriter)
        {
            return mDataWriter->GetLength();
        }
        return 0;
    }

    const std::string& GetPath() const override
    {
        if (mDataWriter)
        {
            return mDataWriter->GetPath();
        }
        static std::string emptyPath = "";
        return emptyPath;
    }

    void Close() override;
    void ReserveFileNode(size_t reserveSize) override
    {
        if (mDataWriter)
        {
            mDataWriter->ReserveFileNode(reserveSize);
        }
    }

    size_t GetUncompressLength() const { return mWriteLength; }
    FileWriterPtr GetDataFileWriter() const { return mDataWriter; }
        
private:
    void DumpCompressData();
    void DumpCompressAddressMapperData();

    void FlushDataFile();
    void FlushInfoFile();
    
private:
    size_t mBufferSize;
    size_t mWriteLength;
    FileWriterPtr mDataWriter;
    FileWriterPtr mInfoWriter;
    CompressFileAddressMapperPtr mCompFileAddrMapper;
    util::BufferCompressorPtr mCompressor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESS_FILE_WRITER_H
