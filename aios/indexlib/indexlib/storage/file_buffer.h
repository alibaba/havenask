#ifndef __INDEXLIB_FILE_BUFFER_H
#define __INDEXLIB_FILE_BUFFER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(storage);

class FileBuffer 
{
public:
    FileBuffer(uint32_t bufferSize);
    ~FileBuffer();
public:
    void Wait();
    void Notify();
public:
    char* GetBaseAddr() const { return mBuffer; }
    uint32_t GetCursor() const { return mCursor; }
    void SetCursor(uint32_t cursor) { mCursor = cursor; }
    uint32_t GetBufferSize() const { return mBufferSize; }
    size_t GetFreeSpace() const { return mBufferSize - mCursor; }
    void SetBusy() { mBusy = true; }

public:
    // for write
    void CopyToBuffer(const char *src, uint32_t len) 
    {
        assert(len + mCursor <= mBufferSize);
        memcpy(mBuffer + mCursor, src, len);
        mCursor += len;
    }
protected:
    char *mBuffer;
    uint32_t mCursor;
    uint32_t mBufferSize;
    volatile bool mBusy;
    autil::ThreadCond mCond;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileBuffer);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_FILE_BUFFER_H
