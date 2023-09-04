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

#include <cstddef>

#include "lockless_allocator/LocklessStat.h"
#include "lockless_allocator/MallocPoolScope.h"

extern size_t swapPoolMallocCount(size_t count);
extern size_t swapPoolMallocSize(size_t size);
extern int openMallocPool(int intercept_malloc);
extern void closeMallocPool();
extern bool collectMallocStat(metric_stat *metric);
extern int swapPoolModeIntercept(int new_mode);
extern bool hasInterceptorMalloc();
