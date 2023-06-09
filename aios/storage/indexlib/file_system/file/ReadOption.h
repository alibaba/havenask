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

#include "indexlib/file_system/fslib/FslibOption.h"

namespace indexlib { namespace util {
struct BlockAccessCounter;
}} // namespace indexlib::util

namespace future_lite {
class Executor;
}

namespace autil {
class TimeoutTerminator;
}

namespace indexlib { namespace file_system {

struct ReadOption {
    util::BlockAccessCounter* blockCounter = nullptr;
    future_lite::Executor* executor;
    autil::TimeoutTerminator* timeoutTerminator = nullptr;
    int advice = IO_ADVICE_NORMAL;
    bool trace = false;
    bool useInternalExecutor = true;
    ReadOption(autil::TimeoutTerminator* terminator)
        : blockCounter(nullptr)
        , executor(nullptr)
        , timeoutTerminator(terminator)
        , advice(IO_ADVICE_NORMAL)
        , trace(false)
        , useInternalExecutor(true)
    {
    }
    ReadOption()
        : blockCounter(nullptr)
        , executor(nullptr)
        , timeoutTerminator(nullptr)
        , advice(IO_ADVICE_NORMAL)
        , trace(false)
        , useInternalExecutor(true)
    {
    }

    static ReadOption LowLatency()
    {
        ReadOption option;
        option.advice = IO_ADVICE_LOW_LATENCY;
        return option;
    }
};
}} // namespace indexlib::file_system
