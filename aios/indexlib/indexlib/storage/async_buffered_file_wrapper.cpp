#include "indexlib/storage/async_buffered_file_wrapper.h"
#include "indexlib/storage/file_work_item.h"
#include "indexlib/storage/common_file_wrapper.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, AsyncBufferedFileWrapper);

AsyncBufferedFileWrapper::AsyncBufferedFileWrapper(
        File *file, Flag mode, int64_t fileLength, 
        uint32_t bufferSize, const util::ThreadPoolPtr &threadPool)
    : BufferedFileWrapper(file, mode, fileLength, bufferSize)
    , mThreadPool(threadPool)
{
    mSwitchBuffer = new FileBuffer(mBuffer->GetBufferSize());
}

AsyncBufferedFileWrapper::~AsyncBufferedFileWrapper() 
{
    DELETE_AND_SET_NULL(mSwitchBuffer);
}

void AsyncBufferedFileWrapper::Close()
{
    if (!mFile || !mFile->isOpened())
    {
        return;
    }
    mSwitchBuffer->Wait();
    if (mMode != READ)
    {
        Flush();
    }
    FileWrapper::Close();

    if (mThreadPool)
    {
        mThreadPool->CheckException();
    }
}

void AsyncBufferedFileWrapper::Flush() 
{
    assert(mMode != READ);
    mSwitchBuffer->Wait();
    CommonFileWrapper::Write(mFile, mBuffer->GetBaseAddr(), 
                             mBuffer->GetCursor());
}

void AsyncBufferedFileWrapper::LoadBuffer(int64_t blockNum)
{
    uint32_t bufferSize = mBuffer->GetBufferSize();
    if (mCurBlock < 0 || (mCurBlock != blockNum && mCurBlock + 1 != blockNum))
    {
        mSwitchBuffer->Wait(); // do not seek when buffer has not been loaded
        int64_t offset = blockNum * bufferSize;
        mFile->seek(offset, fslib::FILE_SEEK_SET);
        size_t readLen = CommonFileWrapper::Read(
                mFile, mBuffer->GetBaseAddr(), bufferSize);
        mBuffer->SetCursor(readLen);
        Prefetch(blockNum + 1);
    }
    else if (mCurBlock + 1 == blockNum)
    {
        mSwitchBuffer->Wait();
        std::swap(mBuffer, mSwitchBuffer);
        Prefetch(blockNum + 1);
    }
    mCurBlock = blockNum;
}

void AsyncBufferedFileWrapper::DumpBuffer()
{
    mSwitchBuffer->Wait();
    std::swap(mBuffer, mSwitchBuffer);
    WorkItem *item = new WriteWorkItem(mFile, mSwitchBuffer);
    mSwitchBuffer->SetBusy();
    mThreadPool->PushWorkItem(item);
}

void AsyncBufferedFileWrapper::Prefetch(int64_t blockNum) 
{
    if (blockNum * mBuffer->GetBufferSize() >= mFileLength)
    {
        return;
    }
    WorkItem *item = new ReadWorkItem(mFile, mSwitchBuffer);
    mSwitchBuffer->SetBusy();
    mThreadPool->PushWorkItem(item);
}

IE_NAMESPACE_END(storage);

