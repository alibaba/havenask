#include "indexlib/file_system/compress_file_writer.h"
#include "indexlib/file_system/compress_file_info.h"
#include "indexlib/file_system/compress_file_address_mapper.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"
#include "indexlib/util/buffer_compressor/snappy_compressor.h"
#include "indexlib/util/buffer_compressor/buffer_compressor_creator.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CompressFileWriter);

CompressFileWriter::CompressFileWriter() 
    : mBufferSize(0)
    , mWriteLength(0)
{
}

CompressFileWriter::~CompressFileWriter() 
{
}

void CompressFileWriter::Init(const FileWriterPtr& fileWriter,
                              const FileWriterPtr& infoWriter,
                              const string& compressorName, size_t bufferSize)
{
    assert(fileWriter);
    assert(infoWriter);

    mBufferSize = bufferSize;
    mWriteLength = 0;
    mDataWriter = fileWriter;
    mInfoWriter = infoWriter;

    mCompressor.reset(BufferCompressorCreator::CreateBufferCompressor(compressorName));
    if (!mCompressor)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "create buffer compressor [%s] failed!",
                             compressorName.c_str());
    }
    
    mCompressor->SetBufferInLen(mBufferSize);
    mCompressor->SetBufferOutLen(mBufferSize);
    mCompFileAddrMapper.reset(new CompressFileAddressMapper);
    mCompFileAddrMapper->InitForWrite(bufferSize);
}

size_t CompressFileWriter::Write(const void* buffer, size_t length)
{
    assert(mCompressor);
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true)
    {
        uint32_t leftLenInBuffer = mBufferSize - mCompressor->GetBufferInLen();
        uint32_t lengthToWrite = (leftLenInBuffer < leftLen) ? leftLenInBuffer : leftLen;
        mCompressor->AddDataToBufferIn(cursor, lengthToWrite);
        cursor += lengthToWrite;
        mWriteLength += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            break;
        }

        if (mCompressor->GetBufferInLen() == mBufferSize)
        {
            DumpCompressData();
        }
    }
    return length;
}

void CompressFileWriter::Close()
{
    if (!mCompressor || !mCompFileAddrMapper)
    {
        // already close
        return;
    }

    if (mDataWriter)
    {
        FlushDataFile();
        FlushInfoFile();
        mCompressor.reset();
        mCompFileAddrMapper.reset();
    }
}

void CompressFileWriter::FlushDataFile()
{
    DumpCompressData();
    DumpCompressAddressMapperData();
    mDataWriter->Close();
}

void CompressFileWriter::FlushInfoFile()
{
    IE_LOG(INFO, "flush compress info file[%s].", mInfoWriter->GetPath().c_str());

    assert(mInfoWriter);
    CompressFileInfo infoFile;
    infoFile.compressorName = mCompressor->GetCompressorName();
    infoFile.blockCount = mCompFileAddrMapper->GetBlockCount();
    infoFile.blockSize = mBufferSize;
    infoFile.deCompressFileLen = mWriteLength;
    infoFile.compressFileLen = mCompFileAddrMapper->GetCompressFileLength();

    string content = infoFile.ToString();
    mInfoWriter->Write(content.c_str(), content.size());
    mInfoWriter->Close();
}

void CompressFileWriter::DumpCompressData()
{
    assert(mCompressor);
    if (mCompressor->GetBufferInLen() == 0)
    {
        // empty buffer
        return;
    }

    if (!mCompressor->Compress())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "compress fail!");
        return;
    }
    
    assert(mDataWriter);
    size_t writeLen = mCompressor->GetBufferOutLen();
    size_t ret = mDataWriter->Write(mCompressor->GetBufferOut(), writeLen);
    if (ret != writeLen)
    {
        mCompressor->Reset();
        INDEXLIB_FATAL_ERROR(FileIO, "write compress data fail!");
        return;
    }
    assert(mCompFileAddrMapper);
    mCompFileAddrMapper->AddOneBlock(writeLen);
    mCompressor->Reset();
}

void CompressFileWriter::DumpCompressAddressMapperData()
{
    assert(mCompFileAddrMapper);
    assert(mDataWriter);
    mCompFileAddrMapper->Dump(mDataWriter);
}

IE_NAMESPACE_END(file_system);

