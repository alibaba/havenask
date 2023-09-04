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
#ifndef FSLIB_IO_CONTROLLER_H_
#define FSLIB_IO_CONTROLLER_H_
#include "fslib/common/common_type.h"

namespace future_lite {
class Executor;
class IOExecutor;
} // namespace future_lite

namespace fslib {

class IOController {
private:
    static constexpr int64_t DEFAULT_TIMEOUT = -1;

public:
    IOController() { Reset(); }
    ~IOController() {}
    IOController(const IOController &) = delete;
    IOController &operator=(const IOController &) = delete;

    void Reset() {
        timeout = DEFAULT_TIMEOUT;
        advice = ADVICE_NORMAL;
        errorCode = EC_OK;
        ioSize = 0;
        executor = nullptr;
    }

    // unit in us
    void setTimeout(int64_t val) { timeout = val; }
    int64_t getTimeout() const { return timeout; }

    void setErrorCode(ErrorCode val) { errorCode = val; }
    ErrorCode getErrorCode() const { return errorCode; }

    void setIoSize(size_t val) { ioSize = val; }
    size_t getIoSize() const { return ioSize; }

    enum ADVICE {
        ADVICE_NORMAL = 0x0,      // No special treatment.  This is the default.
        ADVICE_LOW_LATENCY = 0x1, // Response as quickly as possible
    };

    void setAdvice(int advice_) {
        if (advice_ == ADVICE_NORMAL || advice_ == ADVICE_LOW_LATENCY) {
            advice = advice_;
        }
    };
    int getAdvice() { return advice; }

    void setExecutor(future_lite::Executor *ex) noexcept { executor = ex; }
    future_lite::Executor *getExecutor() noexcept { return executor; }

private:
    // options
    int64_t timeout;
    int advice;
    // response
    ErrorCode errorCode;
    size_t ioSize;
    future_lite::Executor *executor;
};

} // namespace fslib
#endif
