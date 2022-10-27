#include "indexlib/storage/write_control_file_wrapper.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, WriteControlFileWrapper);

WriteControlFileWrapper::WriteControlFileWrapper(
        File* file, int64_t flushTimeInterval, int64_t flushSize)
    : FileWrapper(file)
    , mFlushTimeInterval(flushTimeInterval)
    , mFlushSize(flushSize)
    , mLastFlushTime(autil::TimeUtility::currentTime())
    , mUnFlushSize(0)
{
    assert(mFlushSize > 0);
}

WriteControlFileWrapper::~WriteControlFileWrapper() 
{
}

void WriteControlFileWrapper::Close()
{
    Flush();
    FileWrapper::Close();
}

void WriteControlFileWrapper::CheckFlush()
{
    if (mUnFlushSize < mFlushSize)
    {
        return;
    }
    
    int64_t interval = mFlushTimeInterval * mUnFlushSize / mFlushSize;
    int64_t currentTime = autil::TimeUtility::currentTime();
    if (currentTime < mLastFlushTime + interval) {
        usleep(mLastFlushTime + interval - currentTime);
    }        
    Flush();
    mLastFlushTime = currentTime;
    mUnFlushSize = 0;
}

void WriteControlFileWrapper::Write(const void* buffer, size_t length)
{
    assert(mFile);
    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) 
    {
        size_t writeLen = leftToWriteLen > DEFAULT_READ_WRITE_LENGTH 
                          ? DEFAULT_READ_WRITE_LENGTH : leftToWriteLen;
        ssize_t actualWriteLen = mFile->write(
                (uint8_t*)buffer + totalWriteLen, writeLen);
        if (-1 == actualWriteLen) 
        {
            INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                    "Write data to file: [%s] FAILED",
                    mFile->getFileName());
        }
        leftToWriteLen -= actualWriteLen;
        totalWriteLen += actualWriteLen;
        
        mUnFlushSize += actualWriteLen;
        CheckFlush();
    }
}

IE_NAMESPACE_END(storage);

