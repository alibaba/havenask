#include "indexlib/storage/file_buffer.h"

using namespace std;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, FileBuffer);

FileBuffer::FileBuffer(uint32_t bufferSize) 
{
    mBuffer = new char[bufferSize];
    mCursor = 0;
    mBufferSize = bufferSize;
    mBusy = false;
}

FileBuffer::~FileBuffer() 
{
    delete [] mBuffer;
}

void FileBuffer::Wait() 
{
    autil::ScopedLock lock(mCond);
    while (mBusy) 
    {
        mCond.wait();
    }
}

void FileBuffer::Notify()
{
    autil::ScopedLock lock(mCond);
    mBusy = false;
    mCond.signal();
}

IE_NAMESPACE_END(storage);

