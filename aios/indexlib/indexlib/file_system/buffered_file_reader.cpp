#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/file_system/file_work_item.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/thread_pool.h"
#include "indexlib/storage/file_buffer.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileReader);

BufferedFileReader::BufferedFileReader(const FileNodePtr& fileNode,
                                       uint32_t bufferSize)
    : mFileNode(fileNode)
    , mFileLength(fileNode->GetLength())
    , mOffset(0)
    , mCurBlock(-1)
    , mBuffer(NULL)
    , mSwitchBuffer(NULL)
{
    assert(fileNode);
    ResetBufferParam(bufferSize, false);
}

BufferedFileReader::BufferedFileReader(uint32_t bufferSize, bool asyncRead)
    : mFileNode()
    , mFileLength(-1)
    , mOffset(0)
    , mCurBlock(-1)
    , mBuffer(new FileBuffer(bufferSize))
    , mSwitchBuffer(NULL)
{
    if (asyncRead)
    {
        mSwitchBuffer = new FileBuffer(bufferSize);
        mThreadPool = FileSystemWrapper::GetThreadPool(fslib::READ);
    }
}

BufferedFileReader::~BufferedFileReader() 
{
    if (mFileNode)
    {
        try
        {
            Close();
        }
        catch(...)
        {}
    }
    DELETE_AND_SET_NULL(mBuffer);
    DELETE_AND_SET_NULL(mSwitchBuffer); 
}

void BufferedFileReader::Open(const std::string& filePath)
{
    mFileNode.reset(new BufferedFileNode());
    mFileNode->Open(filePath, FSOT_BUFFERED);
    mFileLength = mFileNode->GetLength();
}

void BufferedFileReader::ResetBufferParam(size_t bufferSize, bool asyncRead)
{
    DELETE_AND_SET_NULL(mBuffer);
    DELETE_AND_SET_NULL(mSwitchBuffer);
    mBuffer = new FileBuffer(bufferSize);
    if (asyncRead)
    {
        mSwitchBuffer = new FileBuffer(bufferSize);
        mThreadPool = FileSystemWrapper::GetThreadPool(fslib::READ);
    }
    else
    {
        mThreadPool.reset();
    }

    mOffset = 0;
    mCurBlock = -1;
}

size_t BufferedFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    Seek(offset);
    return Read(buffer, length, option);
}

size_t BufferedFileReader::Read(void* buffer, size_t length, ReadOption option)
{
    int64_t leftLen = length;
    uint32_t bufferSize = mBuffer->GetBufferSize();
    int64_t blockNum = mOffset / bufferSize;
    char* cursor = (char*)buffer;
    while (true)
    {
        if (mCurBlock != blockNum)
        {
            LoadBuffer(blockNum, option);
        }

        int64_t dataEndOfCurBlock = (blockNum + 1) * bufferSize < mFileLength ? 
                                    (blockNum + 1) * bufferSize : mFileLength;
        int64_t dataLeftInBlock = dataEndOfCurBlock - mOffset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;
        int64_t offsetInBlock = mOffset - blockNum * bufferSize;
        memcpy(cursor, mBuffer->GetBaseAddr() + offsetInBlock, lengthToCopy);
        cursor += lengthToCopy;
        leftLen -= lengthToCopy;
        mOffset += lengthToCopy;
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
    return 0;
}

ByteSliceList* BufferedFileReader::Read(size_t length, size_t offset, ReadOption option)
{ 
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support Read with ByteSliceList");
    return NULL;
}

void* BufferedFileReader::GetBaseAddress() const
{
    return NULL;
}

size_t BufferedFileReader::GetLength() const
{
    return mFileNode->GetLength();
}

const string& BufferedFileReader::GetPath() const
{
    return mFileNode->GetPath();
}

FSOpenType BufferedFileReader::GetOpenType()
{ 
    return mFileNode->GetOpenType();
}

void BufferedFileReader::Close()
{
    if (mSwitchBuffer)
    {
        mSwitchBuffer->Wait();
    }

    if (mThreadPool)
    {
        mThreadPool->CheckException();
        mThreadPool.reset();
    }
}

void BufferedFileReader::LoadBuffer(int64_t blockNum, ReadOption option)
{
    assert(mCurBlock != blockNum);
    if (mThreadPool)
    {
        AsyncLoadBuffer(blockNum, option);
    }
    else
    {
        SyncLoadBuffer(blockNum, option);
    }
}

void BufferedFileReader::SyncLoadBuffer(int64_t blockNum, ReadOption option)
{
    uint32_t bufferSize = mBuffer->GetBufferSize();
    int64_t offset = blockNum * bufferSize;
    uint32_t readSize = bufferSize;
    if (offset + readSize > mFileLength)
    {
        readSize = mFileLength - offset;
    }

    size_t readLen = DoRead(mBuffer->GetBaseAddr(), readSize, offset, option);

    if (readLen != readSize)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Read data file failed(%s), offset(%lu),"
                             " length to read(%u), return len(%lu).",
                             mFileNode->GetPath().c_str(), offset, 
                             readSize, readLen);
    }
    mCurBlock = blockNum;
}

void BufferedFileReader::AsyncLoadBuffer(int64_t blockNum, ReadOption option)
{
    assert(mSwitchBuffer);
    uint32_t bufferSize = mBuffer->GetBufferSize();
    if (mCurBlock < 0 || (mCurBlock != blockNum && mCurBlock + 1 != blockNum))
    {
        mSwitchBuffer->Wait(); // do not seek when buffer has not been loaded
        int64_t offset = blockNum * bufferSize;
        size_t readLen = DoRead(mBuffer->GetBaseAddr(), bufferSize, offset, option);
        mBuffer->SetCursor(readLen);
        PrefetchBlock(blockNum + 1, option);
    }
    else if (mCurBlock + 1 == blockNum)
    {
        mSwitchBuffer->Wait();
        std::swap(mBuffer, mSwitchBuffer);
        PrefetchBlock(blockNum + 1, option);
    }
    mCurBlock = blockNum;
}

size_t BufferedFileReader::DoRead(void* buffer, size_t length, size_t offset, ReadOption option)
{
    assert(mFileNode);
    return mFileNode->Read(buffer, length, offset, option);
}

void BufferedFileReader::PrefetchBlock(int64_t blockNum, ReadOption option) 
{
    size_t offset = blockNum * mBuffer->GetBufferSize();
    if (offset >= (size_t)mFileLength)
    {
        return;
    }

    WorkItem *item = new ReadWorkItem(mFileNode.get(), mSwitchBuffer, offset, option);
    mSwitchBuffer->SetBusy();
    mThreadPool->PushWorkItem(item);
}

IE_NAMESPACE_END(file_system);

