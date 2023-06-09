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

#include <new>
#include <stdlib.h>

namespace kmonitor_adapter {

struct AlignedThreadData {
    AlignedThreadData() = default;
    virtual ~AlignedThreadData() {}

    // use align allocation to avoid false sharing problem.
    // use 128 bytes(x2 L1 cache line size) to avoid false sharing trigged by spacial prefecher
    // see section 2.3.5.4 of 'Intel 64 and IA-32 Architectures Optimization Reference Manual'
    // (Order number : 248966-033)
    void* operator new(size_t size)
    {
        void* p;
        int ret = posix_memalign(&p, size_t(128), size);

        if (ret != 0) {
            throw std::bad_alloc();
        }
        return p;
    }

    void operator delete(void* p) { free(p); }

    virtual void atExit() = 0;
    virtual void destroy() { delete this; }
};

} // namespace kmonitor_adapter
