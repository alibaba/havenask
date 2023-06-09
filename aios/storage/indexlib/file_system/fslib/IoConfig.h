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
#pragma once

#include <stdint.h>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib { namespace file_system {

class IOConfig : public autil::legacy::Jsonizable
{
public:
    IOConfig();
    ~IOConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    uint32_t GetReadBufferSize() const { return enableAsyncRead ? readBufferSize * 2 : readBufferSize; }

    uint32_t GetWriteBufferSize() const { return enableAsyncWrite ? writeBufferSize * 2 : writeBufferSize; }

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IOConfig> IOConfigPtr;
}} // namespace indexlib::file_system
