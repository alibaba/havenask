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
#ifndef FUTURE_LITE_COMMON_H
#define FUTURE_LITE_COMMON_H

#include "future_lite/Log.h"
#include <stdexcept>
#include <stdio.h>
#include <execinfo.h>
#include <unistd.h>
#include <exception>

#define FL_LIKELY(x) __builtin_expect((x), 1)
#define FL_UNLIKELY(x) __builtin_expect((x), 0)

#define FL_INLINE __attribute__((__always_inline__)) inline

#include "future_lite/experimental/coroutine.h"



#if __cpp_coroutines || __cpp_impl_coroutine
#define FUTURE_LITE_COROUTINE_AVAILABLE 1
#endif

namespace future_lite {

inline void logicAssert(bool x, const char* errorMsg)
{
    if (FL_LIKELY(x)) {
        return;
    }
    throw std::logic_error(errorMsg);

}

inline void traceException(std::exception_ptr eptr)
{
    constexpr size_t kMaxArray = 1024;
    size_t size;
    void *array[kMaxArray];
    size = backtrace(array, kMaxArray);
    try {
        std::rethrow_exception(std::current_exception());
    } catch (const std::exception& e) {
        fprintf(stderr, "Exception: %s\n", e.what());
    }
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    fflush(stderr);
}

}

#endif //FUTURE_LITE_COMMON_H
