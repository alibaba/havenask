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
#include <sys/mman.h>
#include <sys/user.h>

#include "autil/CommonMacros.h"
namespace autil {

class MemUtil {
public:
    MemUtil();
    ~MemUtil();
    static uint64_t getMachineTotalMem();

    inline static void markReadOnlyForDebug(void *addr, size_t len) {
#if defined(AUTIL_HAVE_ADDRESS_SANITIZER) || defined(AUTIL_HAVE_MEMORY_SANITIZER)
        if ((size_t)addr % PAGE_SIZE == 0 && (size_t)len % PAGE_SIZE == 0) {
            mprotect(addr, len, PROT_READ);
        }
#endif
    }
    inline static void unmarkReadOnlyForDebug(void *addr, size_t len) {
#if defined(AUTIL_HAVE_ADDRESS_SANITIZER) || defined(AUTIL_HAVE_MEMORY_SANITIZER)
        if ((size_t)addr % PAGE_SIZE == 0 && (size_t)len % PAGE_SIZE == 0) {
            mprotect(addr, len, PROT_READ | PROT_WRITE);
        }
#endif
    }

private:
    MemUtil(const MemUtil &);
    MemUtil &operator=(const MemUtil &);

public:
};

} // namespace autil
