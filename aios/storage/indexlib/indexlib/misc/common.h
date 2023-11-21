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

#include <memory>

#include "autil/CommonMacros.h"

#ifdef AUTIL_HAVE_THREAD_SANITIZER
#include <atomic>
#include <sanitizer/tsan_interface.h>
#include <sanitizer/tsan_interface_atomic.h>
#endif

#define DEFINE_SHARED_PTR(x) typedef std::shared_ptr<x> x##Ptr
#define DYNAMIC_POINTER_CAST(x, y) std::dynamic_pointer_cast<x>(y)

#define CALL_NOT_CONST_FUNC(c, func, args...) const_cast<c*>(this)->func(args)

#define DECLARE_REFERENCE_CLASS(x, y)                                                                                  \
    namespace indexlib::x {                                                                                            \
    class y;                                                                                                           \
    DEFINE_SHARED_PTR(y);                                                                                              \
    }

#define DECLARE_REFERENCE_STRUCT(x, y)                                                                                 \
    namespace indexlib::x {                                                                                            \
    struct y;                                                                                                          \
    DEFINE_SHARED_PTR(y);                                                                                              \
    }

#define GET_IF_NOT_NULL(x, y) (((x) == nullptr) ? nullptr : ((x)->y))

//#define safe_new(type) new (nothrow

#ifdef INDEXLIB_COMMON_PERFTEST
#define MEMORY_BARRIER()                                                                                               \
    {                                                                                                                  \
        __asm__ __volatile__("" ::: "memory");                                                                         \
        usleep(1);                                                                                                     \
    }
#elif defined(AUTIL_HAVE_THREAD_SANITIZER)
#define MEMORY_BARRIER()                                                                                               \
    {                                                                                                                  \
        __tsan_atomic_thread_fence(__tsan_memory_order_seq_cst);                                                       \
        std::atomic_thread_fence(std::memory_order_seq_cst);                                                           \
    }
#else
#define MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory")
#endif

#ifdef NDEBUG
#define __ALWAYS_INLINE __attribute__((always_inline))
#else
#define __ALWAYS_INLINE
#endif

#define INDEXLIB_CONCATE_IMPL(a, b) a##b
#define INDEXLIB_CONCATE(a, b) INDEXLIB_CONCATE_IMPL(a, b)
