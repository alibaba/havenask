#ifndef __INDEXLIB_IO_CONFIG_H
#define __INDEXLIB_IO_CONFIG_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(storage);

class IOConfig : public autil::legacy::Jsonizable
{
public:
    IOConfig();
    ~IOConfig();
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    uint32_t GetReadBufferSize() const
    {
        return enableAsyncRead ? readBufferSize * 2 : readBufferSize;
    }

    uint32_t GetWriteBufferSize() const
    {
        return enableAsyncWrite ? writeBufferSize * 2 : writeBufferSize;
    }
public:
    bool enableAsyncRead;
    bool enableAsyncWrite;
    uint32_t readBufferSize;
    uint32_t writeBufferSize;
    uint32_t readThreadNum;
    uint32_t readQueueSize;
    uint32_t writeThreadNum;
    uint32_t writeQueueSize;

public:
    static const uint32_t DEFAULT_READ_BUFFER_SIZE;
    static const uint32_t DEFAULT_WRITE_BUFFER_SIZE;
    static const uint32_t DEFAULT_READ_THREAD_NUM;
    static const uint32_t DEFAULT_READ_QUEUE_SIZE;
    static const uint32_t DEFAULT_WRITE_THREAD_NUM;
    static const uint32_t DEFAULT_WRITE_QUEUE_SIZE;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IOConfig);
IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_IO_CONFIG_H
