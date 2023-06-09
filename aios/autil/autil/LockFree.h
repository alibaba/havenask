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

#include <sys/epoll.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <syscall.h>

/* copy from asm-x86_64/unistd.h */
#ifndef __NR_eventfd
#define __NR_eventfd        284
#endif

namespace autil {

namespace detail {

    const uint32_t kLockFreeNullPointer = 0xFFFFFFFF;
#if __x86_64__
    const uint64_t kX64AddressSignBit = 0x0000800000000000;
#elif __aarch64__
    const uint64_t kX64AddressSignBit = 0x0000000000000000;
#endif

    const uint16_t kInvalidTag = 0xdead;

    typedef uint64_t Pointer;
    inline uint64_t MakePointer(uint32_t index, uint32_t tag)
    {
        uint64_t value = index;
        value <<= 32;
        value += tag;
        return value;
    }
    inline uint32_t Tag(uint64_t point)
    {
        return point & 0xFFFFFFFF;
    }
    inline uint32_t Index(uint64_t point)
    {
        return point >> 32;
    }
    union TaggedPointer {
        uint64_t mValue;
        void *mPointer;
        struct {
            uint16_t mDummy[3];
            uint16_t mTag;
        };
    };
    inline uint64_t MakePointer(void *address, uint16_t tag)
    {
        TaggedPointer pointer;
        pointer.mValue =
            static_cast<uint64_t>(reinterpret_cast<unsigned long>(address));
        pointer.mTag = tag;
        return pointer.mValue;
    }
    inline void *GetPointer(uint64_t pointer)
    {
        TaggedPointer tagged_pointer;
        tagged_pointer.mValue = pointer;
        tagged_pointer.mTag = (pointer & kX64AddressSignBit) ? 0xFFFF : 0;
        return  reinterpret_cast<void *>(
                    static_cast<unsigned long>(tagged_pointer.mValue));
    }
    inline uint16_t GetTag(uint64_t pointer)
    {
        TaggedPointer tagged_pointer;
        tagged_pointer.mValue = pointer;
        return tagged_pointer.mTag;
    }
    inline uint16_t GetNextTag(uint64_t pointer) {
        TaggedPointer tagged_pointer;
        tagged_pointer.mValue = pointer;
        uint16_t tag = tagged_pointer.mTag + 1;
        if (__builtin_expect(tag == kInvalidTag, 0)) {
            ++tag;
        }
        return tag;
    }
    inline uint16_t GetPrevTag(uint64_t pointer) {
        TaggedPointer tagged_pointer;
        tagged_pointer.mValue = pointer;
        uint16_t tag = tagged_pointer.mTag - 1;
        if (__builtin_expect(tag == kInvalidTag, 0)) {
            --tag;
        }
        return tag;
    }
}

template<typename T>
inline bool AtomicCompareExchange(volatile T *target, T exchange, T compare)
{
    return __sync_bool_compare_and_swap(target, compare, exchange);
}

template<typename T>
inline T AtomicIncrement(volatile T *target)
{
    return __sync_add_and_fetch(target, static_cast<T>(1));
}

template<typename T>
inline T AtomicDecrement(volatile T *target)
{
    return __sync_sub_and_fetch(target, static_cast<T>(1));
}

template<typename T>
inline T AtomicGet(volatile T *target)
{
    T value = *target;
    return value;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
#define arpc_safe_close(fd) {if((fd)>=0){close((fd));(fd)=-1;}}
inline int arpc_eventfd(unsigned int initval, int flags)
{
    return syscall(__NR_eventfd, initval, flags);
}

} // namespace autil
