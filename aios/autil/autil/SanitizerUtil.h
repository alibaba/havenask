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
#include "autil/CommonMacros.h"

#ifdef AUTIL_HAVE_ADDRESS_SANITIZER
#include <sanitizer/asan_interface.h>
#endif

#ifdef AUTILL_HAVE_MEMORY_SANITIZER
#include <sanitizer/msan_interface.h>
#endif

namespace autil::SanitizerUtil {
// Helper functions for asan and msan.
inline void PoisonMemoryRegion(const void *m, size_t s) {
#ifdef AUTIL_HAVE_ADDRESS_SANITIZER
    __asan_poison_memory_region(m, s);
#endif
#ifdef AUTIL_HAVE_MEMORY_SANITIZER
    __msan_poison(m, s);
#endif
    (void)m;
    (void)s;
}

inline void UnpoisonMemoryRegion(const void *m, size_t s) {
#ifdef AUTIL_HAVE_ADDRESS_SANITIZER
    __asan_unpoison_memory_region(m, s);
#endif
#ifdef AUTIL_HAVE_MEMORY_SANITIZER
    __msan_unpoison(m, s);
#endif
    (void)m;
    (void)s;
}

template <typename T>
inline void PoisonObject(const T *object) {
    PoisonMemoryRegion(object, sizeof(T));
}

template <typename T>
inline void UnpoisonObject(const T *object) {
    UnpoisonMemoryRegion(object, sizeof(T));
}

} // namespace autil::SanitizerUtil
