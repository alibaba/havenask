/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/file_system/fslib/IoConfig.h"

#include <cstdint>
#include <iosfwd>

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, IOConfig);

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
{
}

IOConfig::~IOConfig() {}

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
}} // namespace indexlib::file_system
