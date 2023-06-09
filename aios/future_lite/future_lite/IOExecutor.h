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
#ifndef FUTURE_LITE_IO_EXECUTOR_H
#define FUTURE_LITE_IO_EXECUTOR_H


#include <functional>
#include <string>
#include <sys/uio.h>

namespace future_lite {

enum iocb_cmd {
    IOCB_CMD_PREAD = 0,
    IOCB_CMD_PWRITE = 1,
    IOCB_CMD_PREADV = 7,
    IOCB_CMD_PWRITEV = 8,
};

using AIOCallback = std::function<void(int32_t)>;

class IOExecutor {
public:
    using Func = std::function<void()>;

    IOExecutor() {} 
    virtual ~IOExecutor() {}

    IOExecutor(const IOExecutor &) = delete;
    IOExecutor& operator = (const IOExecutor &) = delete;

public:
    virtual void submitIO(int fd, iocb_cmd cmd, void* buffer, size_t length, off_t offset,
                               AIOCallback cbfn) = 0;
    virtual void submitIOV(int fd, iocb_cmd cmd, const iovec* iov, size_t count, off_t offset,
                                AIOCallback cbfn) = 0;
    
    // add stat info
    // virtual IoExecutorStat stat() const = 0;
};

}  // namespace future_lite

#endif
