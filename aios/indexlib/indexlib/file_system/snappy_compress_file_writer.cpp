#include "indexlib/file_system/snappy_compress_file_writer.h"
#include "indexlib/file_system/compress_file_meta.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SnappyCompressFileWriter);

SnappyCompressFileWriter::SnappyCompressFileWriter() 
    : mBufferSize(0)
    , mWriteLength(0)
{
}

SnappyCompressFileWriter::~SnappyCompressFileWriter() 
{
}

void SnappyCompressFileWriter::Init(
        const FileWriterPtr& fileWriter, size_t bufferSize)
{
    assert(fileWriter);
    mBufferSize = bufferSize;
    mWriteLength = 0;
    assert(fileWriter);
    mDataWriter = fileWriter;

    mCompressor.SetBufferInLen(mBufferSize);
    mCompressor.SetBufferOutLen(mBufferSize);
    mCompressMeta.reset(new CompressFileMeta);
}

size_t SnappyCompressFileWriter::Write(const void* buffer, size_t length)
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true)
    {
        uint32_t leftLenInBuffer = mBufferSize - mCompressor.GetBufferInLen();
        uint32_t lengthToWrite = (leftLenInBuffer < leftLen) ?
                                 leftLenInBuffer : leftLen;
        mCompressor.AddDataToBufferIn(cursor, lengthToWrite);
        cursor += lengthToWrite;
        mWriteLength += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            break;
        }

        if (mCompressor.GetBufferInLen() == mBufferSize)
        {
            DumpCompressData();
        }
    }
    return length;
}

void SnappyCompressFileWriter::Close()
{
    if (!mCompressMeta)
    {
        // already close
        return;
    }

    if (mDataWriter)
    {
        DumpCompressData();
        DumpCompressMeta();
        mDataWriter->Close();
        mCompressMeta.reset();
    }
}

void SnappyCompressFileWriter::DumpCompressData()
{
    if (mCompressor.GetBufferInLen() == 0)
    {
        // empty buffer
        return;
    }

    if (!mCompressor.Compress())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "compress fail!");
        return;
    }
    
    assert(mDataWriter);
    size_t writeLen = mCompressor.GetBufferOutLen();
    size_t ret = mDataWriter->Write(mCompressor.GetBufferOut(), writeLen);
    if (ret != writeLen)
    {
        mCompressor.Reset();
        INDEXLIB_FATAL_ERROR(FileIO, "write compress data fail!");
        return;
    }
    assert(mCompressMeta);
    mCompressMeta->AddOneBlock(mWriteLength, mDataWriter->GetLength());
    mCompressor.Reset();
}

void SnappyCompressFileWriter::DumpCompressMeta()
{
    assert(mCompressMeta);
    assert(mDataWriter);
    size_t compressBlockCount = mCompressMeta->GetBlockCount();
    if (compressBlockCount > 0)
    {
        mDataWriter->Write(mCompressMeta->Data(), mCompressMeta->Size());
        mDataWriter->Write(&mBufferSize, sizeof(mBufferSize));
        mDataWriter->Write(&compressBlockCount, sizeof(compressBlockCount));
    }
}

IE_NAMESPACE_END(file_system);

