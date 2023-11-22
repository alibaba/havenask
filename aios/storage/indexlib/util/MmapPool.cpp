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
#include "indexlib/util/MmapPool.h"

#include <cassert>
#include <cstddef>

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace util {

MmapPool::MmapPool() {}

MmapPool::~MmapPool() {}

void* MmapPool::allocate(size_t numBytes) { return _allocator.allocate(numBytes); }

void MmapPool::deallocate(void* ptr, size_t size) { _allocator.deallocate(ptr, size); }

void MmapPool::release() { assert(false); }

size_t MmapPool::reset()
{
    assert(false);
    return 0;
}

bool MmapPool::isInPool(const void* ptr) const
{
    assert(false);
    return false;
}
}} // namespace indexlib::util
