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
#include "lockless_allocator/MallocPoolScope.h"
#include "lockless_allocator/LocklessApi.h"
#include "dlfcn.h"
#include "stddef.h"

// int (*open_malloc_pool_func)(int n) = (int (*)(int))dlsym(NULL, "open_malloc_pool");
// void (*close_malloc_pool_func)(void) = (void (*)())dlsym(NULL, "close_malloc_pool");

MallocPoolScope::MallocPoolScope(bool intercept) {
    openMallocPool(intercept);
}

MallocPoolScope::~MallocPoolScope() {
    closeMallocPool();
}

DisablePoolScope::DisablePoolScope() {
    _poolMode = swapPoolModeIntercept(0);
}

DisablePoolScope::~DisablePoolScope() {
    if (_poolMode != -1) {
        (void)swapPoolModeIntercept(_poolMode);
    }
}
