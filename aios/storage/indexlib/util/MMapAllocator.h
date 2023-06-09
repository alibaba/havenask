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

#include <cstring>
#include <memory>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/mem_pool/ChunkAllocatorBase.h"

namespace indexlib { namespace util {

class MMapAllocator : public autil::mem_pool::ChunkAllocatorBase
{
public:
    MMapAllocator();
    ~MMapAllocator();

public:
    void* doAllocate(size_t numBytes) override { return Mmap(numBytes); }
    void doDeallocate(void* const addr, size_t numBytes) override { Munmap(addr, numBytes); }

    static void* Mmap(size_t numBytes)
    {
        void* addr = mmap(0, numBytes, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
        if (unlikely(addr == MAP_FAILED)) {
            AUTIL_LOG(ERROR, "mmap numBytes[%lu] fail, %s.", numBytes, strerror(errno));
            return NULL;
        }
        if (_mmapDontDump) {
            if (-1 == madvise(addr, numBytes, MADV_DONTDUMP)) {
                AUTIL_LOG(WARN, "set MADV_DONTDUMP failed");
            }
        }
        return addr;
    }
    static void Munmap(void* const addr, size_t numBytes)
    {
        char* const base = (char* const)addr;
        for (size_t offset = 0; offset < numBytes; offset += MUNMAP_SLICE_SIZE) {
            size_t actualLen = std::min(numBytes - offset, MUNMAP_SLICE_SIZE);
            if (munmap(base + offset, actualLen) < 0) {
                AUTIL_LOG(ERROR, "munmap numBytes[%lu] offset[%lu] fail, %s.", numBytes, offset, strerror(errno));
            }
            usleep(0);
        }
    }

private:
    static const size_t MUNMAP_SLICE_SIZE;
    static bool _mmapDontDump;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MMapAllocator> MMapAllocatorPtr;
}} // namespace indexlib::util
