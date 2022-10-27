#include "indexlib/storage/io_config.h"

using namespace std;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, IOConfig);

const uint32_t IOConfig::DEFAULT_READ_BUFFER_SIZE = (uint32_t)2 * 1024 * 1024;
const uint32_t IOConfig::DEFAULT_WRITE_BUFFER_SIZE = (uint32_t)2 * 1024 * 1024;
const uint32_t IOConfig::DEFAULT_READ_THREAD_NUM = 4;
const uint32_t IOConfig::DEFAULT_READ_QUEUE_SIZE = 10;
const uint32_t IOConfig::DEFAULT_WRITE_THREAD_NUM = 4;
const uint32_t IOConfig::DEFAULT_WRITE_QUEUE_SIZE = 10;

IOConfig::IOConfig() 
    : enableAsyncRead(false)
    , enableAsyncWrite(false)
    , readBufferSize(DEFAULT_READ_BUFFER_SIZE)
    , writeBufferSize(DEFAULT_WRITE_BUFFER_SIZE)
    , readThreadNum(DEFAULT_READ_THREAD_NUM)
    , readQueueSize(DEFAULT_READ_QUEUE_SIZE)
    , writeThreadNum(DEFAULT_WRITE_THREAD_NUM)
    , writeQueueSize(DEFAULT_WRITE_QUEUE_SIZE)
{}

IOConfig::~IOConfig()
{}

void IOConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("enable_async_read", enableAsyncRead, enableAsyncRead);
    json.Jsonize("enable_async_write", enableAsyncWrite, enableAsyncWrite);
    json.Jsonize("read_buffer_size", readBufferSize, readBufferSize);
    json.Jsonize("write_buffer_size", writeBufferSize, writeBufferSize);
    json.Jsonize("read_thread_num", readThreadNum, readThreadNum);
    json.Jsonize("read_queue_size", readQueueSize, readQueueSize);
    json.Jsonize("write_thread_num", writeThreadNum, writeThreadNum);
    json.Jsonize("write_queue_size", writeQueueSize, writeQueueSize);
}

IE_NAMESPACE_END(storage);

