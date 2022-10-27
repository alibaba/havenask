#ifndef __INDEXLIB_ASYNC_BUFFERED_FILE_WRAPPER_H
#define __INDEXLIB_ASYNC_BUFFERED_FILE_WRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/storage/buffered_file_wrapper.h"
#include "indexlib/util/thread_pool.h"

IE_NAMESPACE_BEGIN(storage);

class AsyncBufferedFileWrapper : public BufferedFileWrapper
{
public:
    AsyncBufferedFileWrapper(fslib::fs::File *file, fslib::Flag mode, 
                             int64_t fileLength, uint32_t bufferSize, 
                             const util::ThreadPoolPtr &threadPool);
    ~AsyncBufferedFileWrapper();
public:
    void Flush() override;
    void Close() override;
private:
    void LoadBuffer(int64_t blockNum) override;
    void DumpBuffer() override;
private:
    void Prefetch(int64_t blockNum);
private:
    FileBuffer *mSwitchBuffer;
    util::ThreadPoolPtr mThreadPool;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AsyncBufferedFileWrapper);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ASYNC_BUFFERED_FILE_WRAPPER_H
