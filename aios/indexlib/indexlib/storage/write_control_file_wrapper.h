#ifndef __INDEXLIB_WRITECONTROLFILEWRAPPER_H
#define __INDEXLIB_WRITECONTROLFILEWRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

// TODO: useless now
class WriteControlFileWrapper : public FileWrapper
{
public:
    WriteControlFileWrapper(
            fslib::fs::File* file, 
            int64_t flushTimeInterval = std::numeric_limits<int64_t>::max(),
            int64_t flushSize = std::numeric_limits<int64_t>::max());
    ~WriteControlFileWrapper();
public:
    size_t Read(void* buffer, size_t length) override
    {
        assert(false);
        return 0;
    }
    size_t PRead(void* buffer, size_t length, off_t offset) override
    {
        assert(false);
        return 0;
    }
    void Write(const void* buffer, size_t length) override;
    void Close() override;
private:
    void CheckFlush();
private:
    int64_t mFlushTimeInterval;
    int64_t mFlushSize;
    int64_t mLastFlushTime;
    int64_t mUnFlushSize;
private:
    friend class WriteControlFileWrapperTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(WriteControlFileWrapper);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_WRITECONTROLFILEWRAPPER_H
