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
#include "lockless_allocator/LocklessApi.h"

extern "C" {
    extern __attribute__((weak)) size_t swap_pool_malloc_count(size_t count);
    extern __attribute__((weak)) size_t swap_pool_malloc_size(size_t size);
    extern __attribute__((weak)) int open_malloc_pool(int intercept_malloc);
    extern __attribute__((weak)) void close_malloc_pool(void);
    extern __attribute__((weak)) void collect_malloc_stat(metric_stat *metric);
    extern __attribute__((weak)) int swap_pool_mode_intercept(int new_mode);
    extern __attribute__((weak)) void *__interceptor_malloc(size_t size);
}

size_t swapPoolMallocCount(size_t count) {
    if (swap_pool_malloc_count) {
        return swap_pool_malloc_count(count);
    }
    return 0;
}

size_t swapPoolMallocSize(size_t size) {
    if (swap_pool_malloc_size) {
        return swap_pool_malloc_size(size);
    }
    return 0;
}

int openMallocPool(int intercept_malloc) {
    if (open_malloc_pool) {
        return open_malloc_pool((int)intercept_malloc);
    }
    return 0;
}

void closeMallocPool() {
    if (close_malloc_pool) {
        close_malloc_pool();
    }
}

bool collectMallocStat(metric_stat *metric) {
    if (collect_malloc_stat) {
        collect_malloc_stat(metric);
        return true;
    }
    return false;
}

int swapPoolModeIntercept(int new_mode) {
    if (swap_pool_mode_intercept) {
        return swap_pool_mode_intercept(new_mode);
    }
    return -1;
}

bool hasInterceptorMalloc() {
    return __interceptor_malloc != nullptr;
}
