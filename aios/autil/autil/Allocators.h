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

#include <assert.h>
#include <stdlib.h>
#include <sys/mman.h>


namespace autil {

struct SysAllocator
{
    void* allocate(size_t nBytes) {
        return ::malloc(nBytes);
    }
    void deallocate(void *ptr, size_t /*nBytes*/) {
        ::free(ptr);
    }
};

struct MmapAllocator
{
    void* allocate(size_t nBytes) {
        void *ret = mmap(NULL, nBytes, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        return (ret == MAP_FAILED) ? NULL : ret;
    }
    void deallocate(void *ptr, size_t nBytes) {
        int ret = munmap(ptr, nBytes);
        (void) ret; assert(ret == 0);
    }
};

}

