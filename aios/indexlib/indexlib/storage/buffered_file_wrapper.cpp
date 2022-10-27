#include "indexlib/storage/buffered_file_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, BufferedFileWrapper);

BufferedFileWrapper::BufferedFileWrapper(
        File *file, Flag mode, int64_t fileLength, uint32_t bufferSize)
    : FileWrapper(file)
    , mMode(mode)
    , mFileLength(fileLength)
    , mOffset(0)
    , mCurBlock(-1)
{
    mBuffer = new FileBuffer(bufferSize);
}

BufferedFileWrapper::~BufferedFileWrapper() 
{
    DELETE_AND_SET_NULL(mBuffer);
}

size_t BufferedFileWrapper::Read(void* buffer, size_t length)
{
    int64_t leftLen = length;
    uint32_t bufferSize = mBuffer->GetBufferSize();
    int64_t blockNum = mOffset / bufferSize;
    char* cursor = (char*)buffer;
    while (true)
    {
        LoadBuffer(blockNum);
        int64_t dataEndOfCurBlock = (blockNum + 1) * bufferSize < mFileLength ? 
                                    (blockNum + 1) * bufferSize : mFileLength;
        int64_t dataLeftInBlock = dataEndOfCurBlock - mOffset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;
        int64_t offsetInBlock = mOffset - blockNum * bufferSize;
        memcpy(cursor, mBuffer->GetBaseAddr() + offsetInBlock, lengthToCopy);
        leftLen -= lengthToCopy;
        mOffset += lengthToCopy;
        cursor += lengthToCopy;
        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            return length;
        }
        if (mOffset >= mFileLength)
        {
            return (cursor - (char*)buffer);
        }
        blockNum++;
    }
    return -1;
}

void BufferedFileWrapper::Write(const void* buffer, size_t length)
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true)
    {
        uint32_t leftLenInBuffer = mBuffer->GetBufferSize() - mBuffer->GetCursor();
        uint32_t lengthToWrite =  leftLenInBuffer < leftLen ? leftLenInBuffer : leftLen;
        mBuffer->CopyToBuffer(cursor, lengthToWrite);
        cursor += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            mOffset += length;
            break;
        }
        if (mBuffer->GetCursor() == mBuffer->GetBufferSize())
        {
            DumpBuffer();
        }
    }
}

void BufferedFileWrapper::Flush()
{
    assert(mMode != READ);
    DumpBuffer();
}

void BufferedFileWrapper::Close()
{
    if (!mFile || !mFile->isOpened())
    {
        return;
    }
    if (mMode != READ)
    {
        Flush();
    }
    FileWrapper::Close();
}

void BufferedFileWrapper::LoadBuffer(int64_t blockNum)
{
    uint32_t bufferSize = mBuffer->GetBufferSize();
    if (mCurBlock == blockNum)
    {
        return;
    }
    if (mCurBlock == -1 || (mCurBlock + 1) != blockNum)
    {
        int64_t offset = blockNum * bufferSize;
        mFile->seek(offset, fslib::FILE_SEEK_SET);
    }
    size_t readLen = CommonFileWrapper::Read(
            mFile, mBuffer->GetBaseAddr(), bufferSize);
    if (readLen != bufferSize && !mFile->isEof())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Read data file failed(%s), offset(%lu),"
                             " length to read(%u), return len(%ld).",
                             mFile->getFileName(), blockNum * bufferSize, 
                             bufferSize, readLen);
    }
    mCurBlock = blockNum;
}

void BufferedFileWrapper::DumpBuffer() 
{
    CommonFileWrapper::Write(mFile, mBuffer->GetBaseAddr(), mBuffer->GetCursor());
    mBuffer->SetCursor(0);
}

size_t BufferedFileWrapper::PRead(void* buffer, size_t length, off_t offset) 
{
    INDEXLIB_THROW(misc::UnSupportedException, "AsyncRead not supported");
    return 0;
}

void BufferedFileWrapper::Seek(int64_t offset, SeekFlag flag)
{
    INDEXLIB_THROW(misc::UnSupportedException, "AsyncRead not supported");
}

IE_NAMESPACE_END(storage);

